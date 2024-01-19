use std::cell::RefCell;

use hydro_deploy::{Deployment, HydroflowCrate};
use hydroflow_plus_cli_integration::{DeployProcessSpec, DeployClusterSpec};

const NUM_PROCESSES: usize = 1;

#[tokio::main]
async fn main() {
    let mut deployment = Deployment::new();
    let localhost = deployment.Localhost();

    let flow = hydroflow_plus::FlowBuilder::new();
    let deployment = RefCell::new(deployment);
    flow::first_ten_distributed::first_ten_distributed(
        &flow,
        &DeployProcessSpec::new(|| {
            deployment.borrow_mut().add_service(
                HydroflowCrate::new(".", localhost.clone())
                    .bin("first_ten_distributed")
                    .profile("dev"),
            )
        }),
        &DeployClusterSpec::new(|| {
            let mut deployment = deployment.borrow_mut();

            let mut processes = Vec::new();
            for _ in 0..NUM_PROCESSES {
                processes.push(deployment.add_service(
                    HydroflowCrate::new(".", localhost.clone())
                        .bin("first_ten_distributed")
                        .profile("dev"),
                ))
            }

            processes
        }),
    );

    let mut deployment = deployment.into_inner();

    deployment.deploy().await.unwrap();

    deployment.start().await.unwrap();

    tokio::signal::ctrl_c().await.unwrap()
}
