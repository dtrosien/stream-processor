#[cfg(test)]
pub mod test {
    use std::collections::HashMap;
    use std::marker::PhantomData;
    use std::sync::Arc;
    use tokio::sync::Mutex;

    use stream_processor::domain::test::Person;
    use stream_processor::transformation::Transformation;
    use tracing::{error, info, trace, warn};

    #[tracing::instrument(skip_all)]
    pub async fn person_to_string(person: Person, _p: PhantomData<str>) -> String {
        info!("transform person: {}", person.name);
        person.name
    }

    #[derive(Clone)]
    pub struct SomeTransform {
        pub list: Vec<String>,
    }

    impl Transformation<Person, Person> for SomeTransform {
        async fn apply(&mut self, p: Person) -> Person {
            self.list.push(p.name.clone());
            trace!("pushed person, list length is now: {:?}", self.list.len());
            p
        }
    }

    #[derive(Clone)]
    pub struct PrintName;

    impl Transformation<Person, Person> for PrintName {
        async fn apply(&mut self, p: Person) -> Person {
            trace!("transform person: {}", p.name);
            p
        }
    }

    #[tracing::instrument(skip_all)]
    pub async fn print_name(
        person: Person,
        shared_state: Option<Arc<Mutex<HashMap<String, String>>>>,
    ) -> Person {
        info!("transform person: {}", person.name);

        if shared_state.is_some() {
            error!("did not expect val")
        }

        person
    }
}
