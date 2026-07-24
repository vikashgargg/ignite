pub mod definition;
pub mod scheduling;

pub mod gen {
    tonic::include_proto!("zelox.task");
}
