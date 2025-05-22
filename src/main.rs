use openssl::ssl::{SslConnector, SslMethod};
use postgres::Client;
use postgres_openssl::MakeTlsConnector;
use std::error;

fn write_n_rows(client: &mut Client, table: &str, count: i32) -> Result<(), Box<dyn error::Error>> {
    assert!(count > 0, "Count must be greater than 0");
    
    let query = format!(
        "CREATE TABLE IF NOT EXISTS {}(id SERIAL PRIMARY KEY, name TEXT NOT NULL, value REAL);",
        table
    );
    client.execute(&query, &[])?;

    let query = format!(
        "INSERT INTO {}(name, value) SELECT LEFT(md5(i::TEXT), 10), random() FROM generate_series(1, $1) s(i);",
        table
    );
    client.execute(&query, &[&count])?;

    Ok(())
}

#[allow(dead_code)]
struct NeonResponse {
    id: i32,
    name: String,
    value: f32,
}

fn make_read_query(client: &mut Client, table: &str) -> Result<Vec<NeonResponse>, Box<dyn error::Error>> {
    let mut ret = Vec::<NeonResponse>::new();
    let query = format!("SELECT * FROM {};", table);
    for row in client.query(&query, &[])? {
        ret.push(NeonResponse {
            id: row.get(0),
            name: row.get(1),
            value: row.get(2),
        });
    }
    Ok(ret)
}

fn main() -> Result<(), Box<dyn error::Error>> {
    let mode = std::env::args().nth(1).unwrap_or_else(|| "read".to_string());
    let neon_username =
        std::env::var("NEON_USERNAME").unwrap_or_else(|_| "neondb_owner".to_string());
    let neon_password =
        std::env::var("NEON_PASSWORD").expect("NEON_PASSWORD must be set to environment variable");
    let table = std::env::var("TABLE_NAME").unwrap_or_else(|_| "test_table".to_string());

    let builder = SslConnector::builder(SslMethod::tls())?;
    let connector: MakeTlsConnector = MakeTlsConnector::new(builder.build());
    let connection_string: String = format!(
        "postgresql://{}:{}@ep-dry-leaf-w3c56xzk-pooler.eastus2.azure.neon.build/neondb?sslmode=require",
        neon_username, neon_password
    );

    let mut client = Client::connect(&connection_string, connector)?;

    match mode.as_str() {
        "read" => {
            println!("Running in read mode");
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
            write_n_rows(&mut client, &table, 1000)?;
        }
        _ => {
            println!("Invalid mode. Use 'read' or 'write'.");
        }
    };

    Ok(())
}
