fn main() {
    println!("{}", greeting());
}

fn greeting() -> &'static str {
    "Overhop!"
}

#[cfg(test)]
mod tests {
    use super::greeting;

    #[test]
    fn greeting_is_stable() {
        assert_eq!(greeting(), "Overhop!");
    }
}
