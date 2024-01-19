// Fun hack: uncomment if using a custom datatype
// use flow as flow_macro;

#[tokio::main]
async fn main() {
    hydroflow_plus::util::cli::launch(|ports| {
        let flow = flow::first_ten_distributed::first_ten_distributed_runtime!(&ports);
        // flow.meta_graph().unwrap().open_mermaid(&Default::default());
        flow
    })
    .await;
}
