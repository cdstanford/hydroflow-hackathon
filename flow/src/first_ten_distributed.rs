use hydroflow_plus::serde::{Serialize, de::DeserializeOwned};
use hydroflow_plus::{*, stream::Async};
use stageleft::*;

pub fn add_one_to_each_element<'a, W, P: Location<'a>>(stream: Stream<'a, i32, W, P>) -> Stream<'a, i32, W, P> {
    stream.map(q!(|n| n + 1))
}

pub fn round_robin_partition<'a, T: Serialize + DeserializeOwned, W, D: Deploy<'a>>(
    stream_of_data: Stream<'a, T, W, D::Process>,
    cluster: &D::Cluster,
) -> Stream<'a, T, Async, D::Cluster> {
    let cluster_ids = cluster.ids();
    stream_of_data
        .enumerate()
        .map(q!(|(i, data)| ((i % cluster_ids.len()) as u32, data)))
        .demux_bincode(cluster)
}

pub fn first_ten_distributed<'a, D: Deploy<'a>>(
    flow: &'a FlowBuilder<'a, D>,
    process_spec: &impl ProcessSpec<'a, D>,
    cluster_spec: &impl ClusterSpec<'a, D>,
) {
    let process = flow.process(process_spec);
    let cluster = flow.cluster(cluster_spec);

    let numbers = process.source_iter(q!(0..10));
    // add_one_to_each_element(numbers)
    //     .broadcast_bincode(&cluster)
    //     .for_each(q!(|n| println!("{}", n)));

    round_robin_partition::<_, _, D>(numbers, &cluster)
        .for_each(q!(|n| println!("{}", n)));
}

use hydroflow_plus::util::cli::HydroCLI;
use hydroflow_plus_cli_integration::{CLIRuntime, HydroflowPlusMeta};

#[stageleft::entry]
pub fn first_ten_distributed_runtime<'a>(
    flow: &'a FlowBuilder<'a, CLIRuntime>,
    cli: RuntimeData<&'a HydroCLI<HydroflowPlusMeta>>,
) -> impl Quoted<'a, Hydroflow<'a>> {
    first_ten_distributed(flow, &cli, &cli);
    flow.build(q!(cli.meta.subgraph_id))
}

#[stageleft::runtime]
#[cfg(test)]
mod tests {
    use hydro_deploy::{Deployment, HydroflowCrate};
    use hydroflow_plus::futures::StreamExt;
    use hydroflow_plus_cli_integration::{DeployCrateWrapper, DeployProcessSpec};

    // #[tokio::test]
    // async fn first_ten_distributed() {
    //     let mut deployment = Deployment::new();
    //     let localhost = deployment.Localhost();

    //     let flow = hydroflow_plus::FlowBuilder::new();
    //     let second_process = super::first_ten_distributed(
    //         &flow,
    //         &DeployProcessSpec::new(|| {
    //             deployment.add_service(
    //                 HydroflowCrate::new(".", localhost.clone())
    //                     .bin("first_ten_distributed")
    //                     .profile("dev"),
    //             )
    //         }),
    //     );

    //     deployment.deploy().await.unwrap();

    //     let second_process_stdout = second_process.stdout().await;

    //     deployment.start().await.unwrap();

    //     assert_eq!(
    //         second_process_stdout.take(10).collect::<Vec<_>>().await,
    //         vec!["0", "1", "2", "3", "4", "5", "6", "7", "8", "9"]
    //     );
    // }
}
