#![allow(dead_code)]

use hydroflow_plus::serde::{Serialize, Deserialize, de::DeserializeOwned};
use hydroflow_plus::stream::Windowed;
use hydroflow_plus::{*, stream::Async};
use stageleft::*;

use rand::Rng;
use std::fmt::Debug;

pub fn randomized_partition<'a, T: Serialize + DeserializeOwned, W, D: Deploy<'a>>(
    stream_of_data: Stream<'a, T, W, D::Process>,
    cluster: &D::Cluster,
) -> Stream<'a, T, Async, D::Cluster> {
    let cluster_ids = cluster.ids();
    stream_of_data
        .enumerate()
        .map(q!(|(_, data)| {
            let mut rng = rand::thread_rng();
            (rng.gen_range(0..cluster_ids.len() as u32), data)
        }))
        .demux_bincode(cluster)
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum GossipMessage<T> {
    Real(T),
    Gossip(T),
}

pub fn randomized_gossip<'a, T: Debug + Serialize + DeserializeOwned, W, D: Deploy<'a>>(
    stream_of_data: Stream<'a, (u32, T), W, D::Cluster>,
    cluster: &D::Cluster,
) -> Stream<'a, (u32, GossipMessage<T>), Async, D::Cluster> {
    let cluster_ids = cluster.ids();
    let real_messages = stream_of_data
        .map(q!(|(id, data)| {
            println!("id: {}, real message: {:?}", id, data);
            (id, GossipMessage::Real(data))
        }))
        .demux_bincode_tagged(cluster);
    let gossip_messages = stream_of_data
        .map(q!(|(_, data)| {
            let mut rng = rand::thread_rng();
            let target = rng.gen_range(0..cluster_ids.len()) as u32;
            (target, (target, GossipMessage::Gossip(data)))
        }))
        .demux_bincode_interleaved(cluster);
    real_messages.union(&gossip_messages)
}

pub fn add_one_to_each_element<'a, W, P: Location<'a>>(stream: Stream<'a, i32, W, P>) -> Stream<'a, i32, W, P> {
    stream.map(q!(|n| n + 1))
}

pub fn round_robin_partition<'a, T: Serialize + DeserializeOwned, W, D: Deploy<'a>>(
    stream_of_data: Stream<'a, T, W, D::Process>,
    cluster: &D::Cluster,
) -> Stream<'a, (u32, T), Async, D::Cluster> {
    let cluster_ids = cluster.ids();
    stream_of_data
        .enumerate()
        .map(q!(|(i, data)| {
            let id = (i % cluster_ids.len()) as u32;
            (id, (id, data))
        }))
        .demux_bincode(cluster)
}

pub fn distributed_reduce<'a, W, D: Deploy<'a>>(
    stream_of_data: Stream<'a, i128, W, D::Process>,
    cluster: &D::Cluster,
    merge_process: &D::Process,
    // init: T,
    // accum: impl Fn(T, T) -> T + Clone + 'a,
    // merge: impl Fn(T, T) -> T + Clone + 'a,
    // finalize: impl Fn(T) -> T + Clone + 'a,
) -> Stream<'a, i128, Windowed, D::Process> {
    let partitioned = round_robin_partition::<_, _, D>(stream_of_data, cluster);
    let reduced = partitioned
        .map(q!(|(_id, data)| data))
        .tick_batch()

        .reduce(q!(|a, b| {
            // sleep for 1 second
            // std::thread::sleep(std::time::Duration::from_secs(1));
            *a += b
        }))
        .send_bincode_interleaved(merge_process);
    reduced
        .all_ticks()
        .reduce(q!(|a, b| *a += b))
        .map(q!(|data| {
            // Difficult to print time since start_time
            // will be better with support for singleton types
            // print time since start_time
            // println!("Time: {:?}", start_time.elapsed().unwrap());
            data
        }))
}

pub fn first_ten_distributed<'a, D: Deploy<'a>>(
    flow: &'a FlowBuilder<'a, D>,
    process_spec: &impl ProcessSpec<'a, D>,
    cluster_spec: &impl ClusterSpec<'a, D>,
) {
    let process = flow.process(process_spec);
    let cluster = flow.cluster(cluster_spec);

    let numbers = process.source_iter(q!(0..1000000));
    // add_one_to_each_element(numbers)
    //     .broadcast_bincode(&cluster)
    //     .for_each(q!(|n| println!("{}", n)));

    // let partitioned = round_robin_partition::<_, _, D>(numbers, &cluster);
    // randomized_gossip::<_, _, D>(partitioned, &cluster)
    //     .for_each(q!(|n| println!("{:?}", n)));

    distributed_reduce::<_, D>(numbers, &cluster, &process)
        .for_each(q!(|n| println!("{:?}", n)));
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
    // use hydro_deploy::{Deployment, HydroflowCrate};
    // use hydroflow_plus::futures::StreamExt;
    // use hydroflow_plus_cli_integration::{DeployCrateWrapper, DeployProcessSpec};

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
