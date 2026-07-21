use anyhow::{Context, Result, anyhow, bail};
use dotenvy::dotenv;
use reqwest::{Client, StatusCode};
use serde_json::json;
use sqlx::{PgPool, Row};
use std::{env, net::TcpListener, path::Path};
use tokio::{
    fs,
    process::Command,
    time::{Duration, sleep},
};

#[derive(Debug)]
struct DeploymentJob {
    id: i32,
    deployment_id: i32,
    job_type: String,
}

#[derive(Debug)]
struct DeploymentContext {
    project_id: i32,
    project_name: String,
    repo_url: String,
    branch: String,
    app_type: String,
}

#[derive(Debug)]
struct DatabaseConfig {
    engine: String,
    db_name: String,
    db_user: String,
    db_password: String,
    container_name: String,
    internal_host: String,
    port: i32,
}

struct Config {
    workspace_dir: String,
    container_port: u16,
    port_start: u16,
    port_end: u16,
    caddy_admin_url: String,
    caddy_server: String,
    caddy_upstream_host: String,
    base_domain: String,
    public_scheme: String,
}

impl Config {
    fn from_env() -> Result<Self> {
        Ok(Self {
            workspace_dir: env_or("WORKSPACE_DIR", "/tmp/deploynest"),
            container_port: env_or("CONTAINER_PORT", "3000").parse()?,
            port_start: env_or("PORT_START", "3001").parse()?,
            port_end: env_or("PORT_END", "4000").parse()?,
            caddy_admin_url: env_or("CADDY_ADMIN_URL", "http://127.0.0.1:2019")
                .trim_end_matches('/')
                .to_string(),
            caddy_server: env_or("CADDY_SERVER", "srv0"),
            caddy_upstream_host: env_or("CADDY_UPSTREAM_HOST", "host.docker.internal"),
            base_domain: env_or("BASE_DOMAIN", "localhost"),
            public_scheme: env_or("PUBLIC_SCHEME", "http"),
        })
    }
}

const DOCKER_NETWORK: &str = "deploynest-net";

#[tokio::main]
async fn main() -> Result<()> {
    dotenv().ok();

    let pool = PgPool::connect(&env::var("DATABASE_URL")?).await?;
    let config = Config::from_env()?;
    let client = Client::new();
    let poll_interval = env_or("POLL_INTERVAL_SECONDS", "5").parse()?;

    fs::create_dir_all(&config.workspace_dir).await?;
    ensure_docker_network().await?;
    println!("DeployNest worker started");

    loop {
        if let Some(job) = claim_pending_job(&pool).await? {
            println!("Processing job {} (type: {}) for deployment {}", job.id, job.job_type, job.deployment_id);

            let result = match job.job_type.as_str() {
                "deploy" => process_deployment(&pool, &client, &config, &job).await,
                "delete" => process_delete_deployment(&pool, &client, &config, &job).await,
                _ => {
                    eprintln!("Unknown job type: {}", job.job_type);
                    Err(anyhow::anyhow!("Unknown job type: {}", job.job_type))
                }
            };

            if let Err(error) = result {
                eprintln!("Job {} for deployment {} failed: {error:#}", job.id, job.deployment_id);
                let _ = mark_job_failed(&pool, &job, &error.to_string()).await;
            }
        }

        sleep(Duration::from_secs(poll_interval)).await;
    }
}

async fn claim_pending_job(pool: &PgPool) -> Result<Option<DeploymentJob>> {
    let mut transaction = pool.begin().await?;
    let row = sqlx::query(
        r#"
        WITH next_job AS (
            SELECT id, job_type
            FROM deployment_jobs
            WHERE status = 'pending'
            ORDER BY id ASC
            FOR UPDATE SKIP LOCKED
            LIMIT 1
        )
        UPDATE deployment_jobs AS jobs
        SET status = 'running', started_at = NOW(), updated_at = NOW()
        FROM next_job
        WHERE jobs.id = next_job.id
        RETURNING jobs.id, jobs.deployment_id, next_job.job_type
        "#,
    )
    .fetch_optional(&mut *transaction)
    .await?;

    let Some(row) = row else {
        transaction.rollback().await?;
        return Ok(None);
    };

    let job = DeploymentJob {
        id: row.get("id"),
        deployment_id: row.get("deployment_id"),
        job_type: row.get("job_type"),
    };

    sqlx::query("UPDATE deployments SET status = 'running' WHERE id = $1")
        .bind(job.deployment_id)
        .execute(&mut *transaction)
        .await?;
    insert_log_tx(
        &mut transaction,
        job.deployment_id,
        "[WORKER] Deployment started",
    )
    .await?;
    transaction.commit().await?;

    Ok(Some(job))
}

async fn process_deployment(
    pool: &PgPool,
    client: &Client,
    config: &Config,
    job: &DeploymentJob,
) -> Result<()> {
    let deployment = get_deployment_context(pool, job.deployment_id).await?;
    let db_config = get_database_config(pool, deployment.project_id).await?;
    let workspace =
        Path::new(&config.workspace_dir).join(format!("deployment-{}", job.deployment_id));
    let image = format!(
        "deploynest-project-{}:{}",
        deployment.project_id, job.deployment_id
    );
    let container_name = format!("deploynest-{}-{}", deployment.project_id, job.deployment_id);

    if fs::try_exists(&workspace).await? {
        fs::remove_dir_all(&workspace).await?;
    }

    run_logged(
        pool,
        job.deployment_id,
        "git",
        &[
            "clone",
            "--depth",
            "1",
            "--branch",
            &deployment.branch,
            &deployment.repo_url,
            workspace
                .to_str()
                .context("Workspace path is not valid UTF-8")?,
        ],
    )
    .await?;

    let dockerfile = prepare_build_context(&workspace, &deployment.app_type).await?;

    let workspace_path = workspace
        .to_str()
        .context("Workspace path is not valid UTF-8")?;
    let mut build_args = vec!["build", "-t", &image];
    if let Some(dockerfile) = dockerfile.as_deref() {
        build_args.extend(["-f", dockerfile]);
    }
    build_args.push(workspace_path);
    run_logged(pool, job.deployment_id, "docker", &build_args).await?;

    // If a database is configured, ensure the database container is running
    if let Some(ref db_cfg) = db_config {
        insert_log(pool, job.deployment_id, &format!(
            "[DATABASE] Ensuring {} container '{}' is running...",
            db_cfg.engine, db_cfg.container_name
        )).await?;
        ensure_database_container(pool, job.deployment_id, db_cfg).await?;
        update_database_status(pool, deployment.project_id, "running").await?;
    }

    let host_port = available_port(config.port_start, config.port_end)?;
    let port_mapping = format!("{host_port}:{}", config.container_port);
    let project_label = format!("deploynest.project_id={}", deployment.project_id);

    // Build docker run args, injecting DB env vars if configured
    let mut run_args: Vec<String> = vec![
        "run".into(),
        "-d".into(),
        "--restart".into(),
        "unless-stopped".into(),
        "--name".into(),
        container_name.clone(),
        "--label".into(),
        project_label.clone(),
        "--network".into(),
        DOCKER_NETWORK.into(),
        "-p".into(),
        port_mapping,
    ];

    if let Some(ref db_cfg) = db_config {
        let db_connection = match db_cfg.engine.as_str() {
            "mysql" => "mysql",
            "postgresql" => "pgsql",
            _ => "mysql",
        };
        run_args.extend([
            "-e".into(), format!("DB_CONNECTION={db_connection}"),
            "-e".into(), format!("DB_HOST={}", db_cfg.internal_host),
            "-e".into(), format!("DB_PORT={}", db_cfg.port),
            "-e".into(), format!("DB_DATABASE={}", db_cfg.db_name),
            "-e".into(), format!("DB_USERNAME={}", db_cfg.db_user),
            "-e".into(), format!("DB_PASSWORD={}", db_cfg.db_password),
        ]);
    }

    run_args.push(image);

    let run_args_refs: Vec<&str> = run_args.iter().map(|s| s.as_str()).collect();
    let output = run_logged(pool, job.deployment_id, "docker", &run_args_refs).await?;
    let container_id = output.trim().to_string();
    if container_id.is_empty() {
        bail!("Docker did not return a container ID");
    }

    // Run Laravel migrations if database is configured and app is Laravel
    if db_config.is_some() && deployment.app_type == "laravel" {
        insert_log(pool, job.deployment_id, "[DATABASE] Waiting for database to be ready...").await?;
        sleep(Duration::from_secs(10)).await;
        run_migrations(pool, job.deployment_id, &container_name).await?;
    }

    let hostname = format!(
        "{}-{}.{}",
        slugify(&deployment.project_name),
        deployment.project_id,
        config.base_domain
    );

    if let Err(error) =
        configure_caddy(client, config, deployment.project_id, &hostname, host_port).await
    {
        let _ = remove_container(&container_id).await;
        return Err(error);
    }

    let public_url = format!("{}://{}", config.public_scheme, hostname);
    complete_job(pool, job, &container_id, host_port, &public_url).await?;
    remove_old_containers(&project_label, &container_id).await?;
    fs::remove_dir_all(&workspace).await?;
    Ok(())
}

async fn process_delete_deployment(
    pool: &PgPool,
    _client: &Client,
    _config: &Config,
    job: &DeploymentJob,
) -> Result<()> {
    let deployment = get_deployment_context(pool, job.deployment_id).await?;
    let container_name = format!("deploynest-{}-{}", deployment.project_id, job.deployment_id);

    // Stop and remove the app container
    let _ = remove_container(&container_name).await;

    // Delete the deployment from the database completely
    let mut transaction = pool.begin().await?;
    sqlx::query("DELETE FROM deployment_logs WHERE deployment_id = $1")
        .bind(job.deployment_id)
        .execute(&mut *transaction)
        .await?;
    sqlx::query("DELETE FROM deployment_jobs WHERE deployment_id = $1")
        .bind(job.deployment_id)
        .execute(&mut *transaction)
        .await?;
    sqlx::query("DELETE FROM deployments WHERE id = $1")
        .bind(job.deployment_id)
        .execute(&mut *transaction)
        .await?;
    transaction.commit().await?;

    Ok(())
}

// ── Database provisioning ─────────────────────────────────────────────

async fn get_database_config(pool: &PgPool, project_id: i32) -> Result<Option<DatabaseConfig>> {
    let row = sqlx::query(
        r#"
        SELECT engine, db_name, db_user, db_password, container_name, internal_host, port
        FROM project_databases
        WHERE project_id = $1
        "#,
    )
    .bind(project_id)
    .fetch_optional(pool)
    .await?;

    Ok(row.map(|r| DatabaseConfig {
        engine: r.get("engine"),
        db_name: r.get("db_name"),
        db_user: r.get("db_user"),
        db_password: r.get("db_password"),
        container_name: r.get::<String, _>("container_name"),
        internal_host: r.get::<String, _>("internal_host"),
        port: r.get("port"),
    }))
}

async fn ensure_docker_network() -> Result<()> {
    let output = Command::new("docker")
        .args(["network", "ls", "--filter", &format!("name=^{DOCKER_NETWORK}$"), "--format", "{{.Name}}"])
        .output()
        .await?;

    let existing = String::from_utf8_lossy(&output.stdout);
    if existing.trim().is_empty() {
        println!("Creating Docker network: {DOCKER_NETWORK}");
        let status = Command::new("docker")
            .args(["network", "create", DOCKER_NETWORK])
            .status()
            .await?;
        if !status.success() {
            bail!("Failed to create Docker network {DOCKER_NETWORK}");
        }
    }
    Ok(())
}

async fn ensure_database_container(
    pool: &PgPool,
    deployment_id: i32,
    db_cfg: &DatabaseConfig,
) -> Result<()> {
    // Check if the database container is already running
    let output = Command::new("docker")
        .args(["ps", "-q", "--filter", &format!("name=^{}$", db_cfg.container_name)])
        .output()
        .await?;

    if !String::from_utf8_lossy(&output.stdout).trim().is_empty() {
        insert_log(pool, deployment_id, &format!(
            "[DATABASE] Container '{}' is already running",
            db_cfg.container_name
        )).await?;

        // Make sure it's connected to the network
        let _ = Command::new("docker")
            .args(["network", "connect", DOCKER_NETWORK, &db_cfg.container_name])
            .output()
            .await;

        return Ok(());
    }

    // Check if the container exists but is stopped
    let output = Command::new("docker")
        .args(["ps", "-aq", "--filter", &format!("name=^{}$", db_cfg.container_name)])
        .output()
        .await?;

    if !String::from_utf8_lossy(&output.stdout).trim().is_empty() {
        insert_log(pool, deployment_id, &format!(
            "[DATABASE] Starting existing container '{}'...",
            db_cfg.container_name
        )).await?;
        let status = Command::new("docker")
            .args(["start", &db_cfg.container_name])
            .status()
            .await?;
        if !status.success() {
            bail!("Failed to start database container {}", db_cfg.container_name);
        }
        return Ok(());
    }

    // Container doesn't exist, create it
    insert_log(pool, deployment_id, &format!(
        "[DATABASE] Creating new {} container '{}'...",
        db_cfg.engine, db_cfg.container_name
    )).await?;

    let volume_name = format!("{}-data", db_cfg.container_name);

    let mut args: Vec<String> = vec![
        "run".into(),
        "-d".into(),
        "--restart".into(),
        "unless-stopped".into(),
        "--name".into(),
        db_cfg.container_name.clone(),
        "--network".into(),
        DOCKER_NETWORK.into(),
        "-v".into(),
        format!("{volume_name}:/var/lib/{}", if db_cfg.engine == "mysql" { "mysql" } else { "postgresql/data" }),
    ];

    match db_cfg.engine.as_str() {
        "mysql" => {
            args.extend([
                "-e".into(), format!("MYSQL_ROOT_PASSWORD={}", db_cfg.db_password),
                "-e".into(), format!("MYSQL_DATABASE={}", db_cfg.db_name),
                "-e".into(), format!("MYSQL_USER={}", db_cfg.db_user),
                "-e".into(), format!("MYSQL_PASSWORD={}", db_cfg.db_password),
                "mysql:8".into(),
            ]);
        }
        "postgresql" => {
            args.extend([
                "-e".into(), format!("POSTGRES_DB={}", db_cfg.db_name),
                "-e".into(), format!("POSTGRES_USER={}", db_cfg.db_user),
                "-e".into(), format!("POSTGRES_PASSWORD={}", db_cfg.db_password),
                "postgres:16".into(),
            ]);
        }
        other => bail!("Unsupported database engine: {other}"),
    }

    let args_refs: Vec<&str> = args.iter().map(|s| s.as_str()).collect();
    run_logged(pool, deployment_id, "docker", &args_refs).await?;

    insert_log(pool, deployment_id, &format!(
        "[DATABASE] {} container '{}' started successfully",
        db_cfg.engine, db_cfg.container_name
    )).await?;

    Ok(())
}

async fn run_migrations(
    pool: &PgPool,
    deployment_id: i32,
    container_name: &str,
) -> Result<()> {
    insert_log(pool, deployment_id, "[DATABASE] Running Laravel migrations...").await?;

    let output = Command::new("docker")
        .args(["exec", container_name, "php", "artisan", "migrate", "--force"])
        .output()
        .await?;

    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);

    if !stdout.trim().is_empty() {
        insert_log(pool, deployment_id, &stdout).await?;
    }
    if !stderr.trim().is_empty() {
        insert_log(pool, deployment_id, &stderr).await?;
    }

    if output.status.success() {
        insert_log(pool, deployment_id, "[DATABASE] Migrations completed successfully").await?;
    } else {
        insert_log(pool, deployment_id, "[DATABASE] Migration failed (non-fatal, app container still running)").await?;
    }

    Ok(())
}

async fn update_database_status(pool: &PgPool, project_id: i32, status: &str) -> Result<()> {
    sqlx::query("UPDATE project_databases SET status = $1 WHERE project_id = $2")
        .bind(status)
        .bind(project_id)
        .execute(pool)
        .await?;
    Ok(())
}

// ── Existing helpers ──────────────────────────────────────────────────

async fn get_deployment_context(pool: &PgPool, deployment_id: i32) -> Result<DeploymentContext> {
    let row = sqlx::query(
        r#"
        SELECT p.id AS project_id, p.name AS project_name, p.repo_url, p.branch, p.app_type
        FROM deployments d
        JOIN projects p ON p.id = d.project_id
        WHERE d.id = $1
        "#,
    )
    .bind(deployment_id)
    .fetch_one(pool)
    .await?;

    Ok(DeploymentContext {
        project_id: row.get("project_id"),
        project_name: row.get("project_name"),
        repo_url: row.get("repo_url"),
        branch: row.get("branch"),
        app_type: row.get("app_type"),
    })
}

async fn prepare_build_context(workspace: &Path, app_type: &str) -> Result<Option<String>> {
    match app_type {
        "dockerfile" => Ok(None),
        "laravel" => {
            if !fs::try_exists(workspace.join("artisan")).await?
                || !fs::try_exists(workspace.join("composer.json")).await?
            {
                bail!("Laravel projects must contain artisan and composer.json");
            }

            let deploynest_dir = workspace.join(".deploynest");
            fs::create_dir_all(&deploynest_dir).await?;
            let dockerfile = deploynest_dir.join("Dockerfile.laravel");
            fs::write(&dockerfile, include_str!("../templates/laravel.Dockerfile")).await?;
            fs::write(
                deploynest_dir.join("laravel-entrypoint.sh"),
                include_str!("../templates/laravel-entrypoint.sh"),
            )
            .await?;

            Ok(Some(
                dockerfile
                    .to_str()
                    .context("Generated Dockerfile path is not valid UTF-8")?
                    .to_string(),
            ))
        }
        value => bail!("Unsupported application type: {value}"),
    }
}

async fn configure_caddy(
    client: &Client,
    config: &Config,
    project_id: i32,
    hostname: &str,
    port: u16,
) -> Result<()> {
    let route_id = format!("deploynest_project_{project_id}");
    let route = json!({
        "@id": route_id,
        "match": [{ "host": [hostname] }],
        "handle": [{
            "handler": "reverse_proxy",
            "upstreams": [{ "dial": format!("{}:{port}", config.caddy_upstream_host) }]
        }],
        "terminal": true
    });

    let route_url = format!("{}/id/{}", config.caddy_admin_url, route_id);
    let existing_response = client
        .get(&route_url)
        .send()
        .await
        .context("Cannot reach Caddy admin API")?;
    let response = if existing_response.status().is_success() {
        client.patch(route_url).json(&route).send().await
    } else if existing_response.status() == StatusCode::NOT_FOUND {
        let insert_url = format!(
            "{}/config/apps/http/servers/{}/routes/0",
            config.caddy_admin_url, config.caddy_server
        );
        client.put(insert_url).json(&route).send().await
    } else {
        bail!(
            "Caddy rejected route lookup: {}",
            existing_response.text().await?
        );
    }
    .context("Cannot update Caddy proxy route")?;

    if !response.status().is_success() {
        bail!("Caddy rejected proxy route: {}", response.text().await?);
    }

    Ok(())
}

async fn run_logged(
    pool: &PgPool,
    deployment_id: i32,
    program: &str,
    args: &[&str],
) -> Result<String> {
    use std::process::Stdio;
    use tokio::io::{AsyncBufReadExt, BufReader};
    use std::sync::Arc;
    use tokio::sync::Mutex;

    let command_summary = if program == "git" && args.first() == Some(&"clone") {
        "git clone [repository]".to_string()
    } else {
        format!("{program} {}", args.join(" "))
    };
    insert_log(pool, deployment_id, &format!("[COMMAND] {command_summary}")).await?;

    let mut child = Command::new(program)
        .args(args)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .with_context(|| format!("Failed to start {program}"))?;

    let stdout = child.stdout.take().expect("Failed to open stdout");
    let stderr = child.stderr.take().expect("Failed to open stderr");

    let pool_clone1 = pool.clone();
    let pool_clone2 = pool.clone();

    let full_stdout = Arc::new(Mutex::new(String::new()));
    let full_stdout_clone = full_stdout.clone();

    let stdout_handle = tokio::spawn(async move {
        let mut reader = BufReader::new(stdout).lines();
        while let Ok(Some(line)) = reader.next_line().await {
            full_stdout_clone.lock().await.push_str(&format!("{line}\n"));
            let _ = insert_log(&pool_clone1, deployment_id, &line).await;
        }
    });

    let stderr_handle = tokio::spawn(async move {
        let mut reader = BufReader::new(stderr).lines();
        while let Ok(Some(line)) = reader.next_line().await {
            let _ = insert_log(&pool_clone2, deployment_id, &line).await;
        }
    });

    let status = child.wait().await?;
    let _ = stdout_handle.await;
    let _ = stderr_handle.await;

    if !status.success() {
        bail!("{program} exited with {}", status);
    }

    let final_stdout = full_stdout.lock().await.clone();
    Ok(final_stdout)
}

fn available_port(start: u16, end: u16) -> Result<u16> {
    for port in start..=end {
        if TcpListener::bind(("127.0.0.1", port)).is_ok() {
            return Ok(port);
        }
    }
    Err(anyhow!("No available port in range {start}-{end}"))
}

async fn complete_job(
    pool: &PgPool,
    job: &DeploymentJob,
    container_id: &str,
    port: u16,
    domain: &str,
) -> Result<()> {
    let mut transaction = pool.begin().await?;
    sqlx::query("UPDATE deployment_jobs SET status = 'completed', completed_at = NOW(), updated_at = NOW() WHERE id = $1")
        .bind(job.id).execute(&mut *transaction).await?;
    sqlx::query("UPDATE deployments SET status = 'success', container_id = $2, port = $3, domain = $4 WHERE id = $1")
        .bind(job.deployment_id).bind(container_id).bind(i32::from(port)).bind(domain).execute(&mut *transaction).await?;
    insert_log_tx(
        &mut transaction,
        job.deployment_id,
        &format!("[DONE] Deployment available at {domain}"),
    )
    .await?;
    transaction.commit().await?;
    Ok(())
}

async fn mark_job_failed(pool: &PgPool, job: &DeploymentJob, message: &str) -> Result<()> {
    let mut transaction = pool.begin().await?;
    sqlx::query("UPDATE deployment_jobs SET status = 'failed', completed_at = NOW(), updated_at = NOW() WHERE id = $1")
        .bind(job.id).execute(&mut *transaction).await?;
    sqlx::query("UPDATE deployments SET status = 'failed' WHERE id = $1")
        .bind(job.deployment_id)
        .execute(&mut *transaction)
        .await?;
    insert_log_tx(
        &mut transaction,
        job.deployment_id,
        &format!("[ERROR] {message}"),
    )
    .await?;
    transaction.commit().await?;
    Ok(())
}

async fn insert_log(pool: &PgPool, deployment_id: i32, message: &str) -> Result<()> {
    sqlx::query("INSERT INTO deployment_logs (deployment_id, message) VALUES ($1, $2)")
        .bind(deployment_id)
        .bind(message)
        .execute(pool)
        .await?;
    Ok(())
}

async fn insert_log_tx(
    transaction: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    deployment_id: i32,
    message: &str,
) -> Result<()> {
    sqlx::query("INSERT INTO deployment_logs (deployment_id, message) VALUES ($1, $2)")
        .bind(deployment_id)
        .bind(message)
        .execute(&mut **transaction)
        .await?;
    Ok(())
}

async fn remove_old_containers(project_label: &str, current_id: &str) -> Result<()> {
    let output = Command::new("docker")
        .args(["ps", "-aq", "--filter", &format!("label={project_label}")])
        .output()
        .await?;
    for container in String::from_utf8_lossy(&output.stdout).lines() {
        if !current_id.starts_with(container) && !container.starts_with(current_id) {
            remove_container(container).await?;
        }
    }
    Ok(())
}

async fn remove_container(container_id: &str) -> Result<()> {
    let status = Command::new("docker")
        .args(["rm", "-f", container_id])
        .status()
        .await?;
    if !status.success() {
        bail!("Failed to remove old container {container_id}");
    }
    Ok(())
}

fn slugify(value: &str) -> String {
    let slug = value
        .to_ascii_lowercase()
        .chars()
        .map(|character| {
            if character.is_ascii_alphanumeric() {
                character
            } else {
                '-'
            }
        })
        .collect::<String>()
        .split('-')
        .filter(|part| !part.is_empty())
        .collect::<Vec<_>>()
        .join("-");

    if slug.is_empty() {
        "app".to_string()
    } else {
        slug
    }
}

fn env_or(key: &str, default: &str) -> String {
    env::var(key).unwrap_or_else(|_| default.to_string())
}

#[cfg(test)]
mod tests {
    use super::slugify;

    #[test]
    fn creates_dns_safe_project_slug() {
        assert_eq!(slugify(" My Cool_App! "), "my-cool-app");
        assert_eq!(slugify("---"), "app");
    }
}
