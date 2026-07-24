use zelox_common::tests::test_gold_set;
use zelox_sql_parser::ast::statement::Statement;
use zelox_sql_parser::tree::SyntaxGraph;

#[test]
#[expect(clippy::unwrap_used)]
fn test_syntax() {
    test_gold_set(
        "tests/gold_data/syntax.json",
        |()| Ok(SyntaxGraph::build::<Statement>()),
        |e| e,
    )
    .unwrap();
}
