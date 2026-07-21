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
}

#[derive(Debug)]
struct DeploymentContext {
    project_id: i32,
    project_name: String,
    repo_url: String,
    branch: String,
    app_type: String,
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

#[tokio::main]
async fn main() -> Result<()> {
    dotenv().ok();

    let pool = PgPool::connect(&env::var("DATABASE_URL")?).await?;
    let config = Config::from_env()?;
    let client = Client::new();
    let poll_interval = env_or("POLL_INTERVAL_SECONDS", "5").parse()?;

    fs::create_dir_all(&config.workspace_dir).await?;
    println!("DeployNest worker started");

    loop {
        if let Some(job) = claim_pending_job(&pool).await? {
            println!("Processing deployment {}", job.deployment_id);

            if let Err(error) = process_deployment(&pool, &client, &config, &job).await {
                eprintln!("Deployment {} failed: {error:#}", job.deployment_id);
                mark_job_failed(&pool, &job, &error.to_string()).await?;
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
            SELECT id
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
        RETURNING jobs.id, jobs.deployment_id
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

    let host_port = available_port(config.port_start, config.port_end)?;
    let port_mapping = format!("{host_port}:{}", config.container_port);
    let project_label = format!("deploynest.project_id={}", deployment.project_id);
    let output = run_logged(
        pool,
        job.deployment_id,
        "docker",
        &[
            "run",
            "-d",
            "--restart",
            "unless-stopped",
            "--name",
            &container_name,
            "--label",
            &project_label,
            "-p",
            &port_mapping,
            &image,
        ],
    )
    .await?;
    let container_id = output.trim().to_string();
    if container_id.is_empty() {
        bail!("Docker did not return a container ID");
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
    let command_summary = if program == "git" && args.first() == Some(&"clone") {
        "git clone [repository]".to_string()
    } else {
        format!("{program} {}", args.join(" "))
    };
    insert_log(pool, deployment_id, &format!("[COMMAND] {command_summary}")).await?;
    let output = Command::new(program)
        .args(args)
        .output()
        .await
        .with_context(|| format!("Failed to start {program}"))?;

    for line in String::from_utf8_lossy(&output.stdout).lines() {
        insert_log(pool, deployment_id, line).await?;
    }
    for line in String::from_utf8_lossy(&output.stderr).lines() {
        insert_log(pool, deployment_id, line).await?;
    }

    if !output.status.success() {
        bail!("{program} exited with {}", output.status);
    }

    Ok(String::from_utf8_lossy(&output.stdout).to_string())
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
