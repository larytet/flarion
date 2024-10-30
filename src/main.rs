mod greatest;

fn main() -> datafusion::error::Result<()> {
    greatest::run_query()
}