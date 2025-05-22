use openssl::ssl::{SslConnector, SslMethod};
use postgres_openssl::MakeTlsConnector;
use std::error;

fn write_n_rows(client: &mut postgres::Client, table: &str, count: i32) -> Result<(), Box<dyn error::Error>> {
    assert!(count > 0, "Count must be greater than 0");

    println!("Dropping table {table} if exists...");
    let query = format!("DROP TABLE IF EXISTS {table};");
    client.execute(&query, &[])?;

    println!("Creating table {table}...");
    let query =
        format!("CREATE TABLE {table}(id SERIAL PRIMARY KEY, name TEXT NOT NULL, value REAL);");
    client.execute(&query, &[])?;

    println!("Inserting {count} values into {table}...");
    let query = format!(
        "INSERT INTO {table}(name, value) SELECT LEFT(md5(i::TEXT), 10), random() FROM generate_series(1, $1) s(i);"
    );
    client.execute(&query, &[&count])?;

    println!("Inserted {count} rows into table {table}.");
    Ok(())
}

#[allow(dead_code)]
struct NeonResponse {
    id: i32,
    name: String,
    value: f32,
}

fn make_read_query(
    client: &mut postgres::Client,
    table: &str,
) -> Result<Vec<NeonResponse>, Box<dyn error::Error>> {
    let mut ret = Vec::<NeonResponse>::new();
    let query = format!("SELECT * FROM {table};");
    for row in client.query(&query, &[])? {
        ret.push(NeonResponse {
            id: row.get(0),
            name: row.get(1),
            value: row.get(2),
        });
    }
    Ok(ret)
}

fn suspend_compute(api_key: &str, project_id: &str, server_name: &str) -> Result<(), Box<dyn error::Error>> {
    let url = format!(
        "https://console-stage.neon.build/api/v2/projects/{}/endpoints/{}/suspend",
        project_id, server_name
    );
    let client = reqwest::blocking::Client::new();
    let response = client
        .post(&url)
        .bearer_auth(api_key)
        .header(reqwest::header::ACCEPT, "application/json")
        .send()?;

    println!("Suspend status code: {}", response.status());
    println!("Suspend response: {}", response.text()?);

    Ok(())
}

fn main() -> Result<(), Box<dyn error::Error>> {
    let mode = std::env::args()
        .nth(1)
        .unwrap();

    // Set up Neon connection parameters
    let neon_server_name =
        std::env::var("NEON_SERVER_NAME").unwrap_or_else(|_| "ep-soft-mouse-w1azk5bq-pooler".to_string());
    let neon_server_domain =
        std::env::var("NEON_SERVER_DOMAIN").unwrap_or_else(|_| "eu-west-1.aws.neon.build".to_string());
    let neon_endpoint = format!("{neon_server_name}.{neon_server_domain}");

    // Set up Neon credentials
    let neon_username =
        std::env::var("NEON_USERNAME").unwrap_or_else(|_| "neondb_owner".to_string());
    let neon_password =
        std::env::var("NEON_PASSWORD").expect("NEON_PASSWORD must be set to environment variable");
    let neon_api_key =
        std::env::var("NEON_API_KEY").expect("NEON_API_KEY must be set to environment variable");

    // Set up Neon project & table
    let neon_project =
        std::env::var("NEON_PROJECT").unwrap_or_else(|_| "hello-world".to_string());
    let table =std::env::var("TABLE_NAME").unwrap_or_else(|_| "test_table".to_string());

    let connection_string: String = format!(
        "postgresql://{}:{}@{}/neondb?sslmode=require",
        neon_username, neon_password, neon_endpoint
    );

    let builder = SslConnector::builder(SslMethod::tls())?;
    let connector: MakeTlsConnector = MakeTlsConnector::new(builder.build());

    match mode.as_str() {
        "read" => {
            println!("Running in read mode");
            let mut client = postgres::Client::connect(&connection_string, connector)?;
            for i in 1..=10 {
                let start_time = std::time::Instant::now();
                let result = make_read_query(&mut client, &table)?;
                let elapsed_time = start_time.elapsed();
                println!("Run {i}: Elapsed time: {:?}", elapsed_time);
                println!("Run {i}: Number of values: {}", result.len());
            }
        }
        "write" => {
            println!("Running in write mode");
            let mut client = postgres::Client::connect(&connection_string, connector)?;
            write_n_rows(&mut client, &table, 30000)?;
        }
        "suspend" => {
            println!("Running in suspend mode");
            suspend_compute(&neon_api_key, &neon_project, &neon_server_name)?;
        }
        _ => {
            println!("Invalid mode. Use 'read' or 'write'.");
        }
    };

    Ok(())
}
