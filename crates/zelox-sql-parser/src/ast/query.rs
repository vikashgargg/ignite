use chumsky::extra::ParserExtra;
use chumsky::input::{Input, ValueInput};
use chumsky::label::LabelError;
use chumsky::pratt::{infix, left};
use chumsky::prelude::choice;
use chumsky::Parser;
use either::Either;
use zelox_sql_macro::{TreeParser, TreeSyntax, TreeText};

use crate::ast::expression::{
    DuplicateTreatment, Expr, FunctionArgument, GroupingExpr, GroupingSet, OrderByExpr, WindowSpec,
};
use crate::ast::identifier::{column_ident, object_name, table_ident, Ident, ObjectName};
use crate::ast::keywords::{
    All, Anti, As, Bucket, By, Cluster, Cross, Cube, Delay, Distinct, Distribute, Except, Exclude,
    For, From, Full, Group, Grouping, Having, Identifier, In, Include, Inner, Insert, Intersect,
    Into as IntoKeyword, Join, Lateral, Left, Limit, Minus, Name, Natural, Nulls, Of, Offset, On,
    Order, Out, Outer, Partition, Percent, Pivot, Qualify, Recursive, Repeatable, Right,
    Rollup, Rows, Select, Semi, Sets, Sort, SystemTime, SystemVersion, Table, Tablesample,
    Timestamp, Union, Unpivot, Using, Values, Version, View, Watermark, Where, Window, With,
};
use crate::ast::literal::IntegerLiteral;
use crate::ast::operator::{Comma, LeftParenthesis, RightParenthesis};
use crate::combinator::{boxed, compose, either_or, sequence, unit};
use crate::common::Sequence;
use crate::options::ParserOptions;
use crate::span::TokenSpan;
use crate::token::{Token, TokenLabel};
use crate::tree::TreeParser;

#[derive(Debug, Clone, TreeParser, TreeSyntax, TreeText)]
#[parser(dependency = "(Query, Expr, TableWithJoins)", label = TokenLabel::Query)]
pub struct Query {
    #[parser(function = |(q, _, _), o| compose(q, o))]
    pub with: Option<WithClause>,
    #[parser(function = |(q, e, t), o| boxed(compose((q, e, t), o)))]
    pub body: Box<QueryBody>,
    #[parser(function = |(_, e, _), o| compose(e, o))]
    pub modifiers: Vec<QueryModifier>,
}

#[derive(Debug, Clone, TreeParser, TreeSyntax, TreeText)]
#[parser(dependency = "Expr")]
pub enum QueryModifier {
    Window(#[parser(function = |e, o| compose(e, o))] WindowClause),
    OrderBy(#[parser(function = |e, o| compose(e, o))] OrderByClause),
    SortBy(#[parser(function = |e, o| compose(e, o))] SortByClause),
    ClusterBy(#[parser(function = |e, o| compose(e, o))] ClusterByClause),
    DistributeBy(#[parser(function = |e, o| compose(e, o))] DistributeByClause),
    Limit(#[parser(function = |e, o| compose(e, o))] LimitClause),
    Offset(#[parser(function = |e, o| compose(e, o))] OffsetClause),
}

#[derive(Debug, Clone, TreeParser, TreeSyntax, TreeText)]
#[parser(dependency = "Query")]
pub struct WithClause {
    pub with: With,
    pub recursive: Option<Recursive>,
    #[parser(function = |q, o| sequence(compose(q, o), unit(o)))]
    pub ctes: Sequence<NamedQuery, Comma>,
}

#[derive(Debug, Clone, TreeParser, TreeSyntax, TreeText)]
#[parser(dependency = "Query")]
pub struct NamedQuery {
    pub name: Ident,
    pub columns: Option<IdentList>,
    pub r#as: Option<As>,
    pub left: LeftParenthesis,
    #[parser(function = |q, _| q)]
    pub query: Query,
    pub right: RightParenthesis,
}

#[derive(Debug, Clone, TreeParser, TreeSyntax, TreeText)]
pub struct IdentList {
    pub left: LeftParenthesis,
    pub names: Sequence<Ident, Comma>,
    pub right: RightParenthesis,
}

#[derive(Debug, Clone, TreeSyntax, TreeText)]
pub enum QueryBody {
    Term(Box<QueryTerm>),
    SetOperation {
        left: Box<QueryBody>,
        operator: SetOperator,
        quantifier: Option<SetQuantifier>,
        right: Box<QueryBody>,
    },
}

impl<'a, I, E, P1, P2, P3> TreeParser<'a, I, E, (P1, P2, P3)> for QueryBody
where
    I: Input<'a, Token = Token<'a>> + ValueInput<'a>,
    I::Span: Into<TokenSpan> + Clone,
    E: ParserExtra<'a, I> + 'a,
    E::Error: LabelError<'a, I, TokenLabel>,
    P1: Parser<'a, I, Query, E> + Clone + 'a,
    P2: Parser<'a, I, Expr, E> + Clone + 'a,
    P3: Parser<'a, I, TableWithJoins, E> + Clone + 'a,
{
    fn parser(
        (query, expr, table_with_joins): (P1, P2, P3),
        options: &'a ParserOptions,
    ) -> impl Parser<'a, I, Self, E> + Clone {
        let quantifier = SetQuantifier::parser((), options).or_not();
        let term = QueryTerm::parser((query, expr, table_with_joins), options)
            .map(|t| QueryBody::Term(Box::new(t)));
        term.pratt((
            infix(
                left(2),
                Intersect::parser((), options)
                    .map(SetOperator::Intersect)
                    .then(quantifier.clone()),
                |left, (operator, quantifier), right, _| QueryBody::SetOperation {
                    left: Box::new(left),
                    operator,
                    quantifier,
                    right: Box::new(right),
                },
            ),
            infix(
                left(1),
                choice((
                    Union::parser((), options).map(SetOperator::Union),
                    Except::parser((), options).map(SetOperator::Except),
                    Minus::parser((), options).map(SetOperator::Minus),
                ))
                .then(quantifier),
                |left, (operator, quantifier), right, _| QueryBody::SetOperation {
                    left: Box::new(left),
                    operator,
                    quantifier,
                    right: Box::new(right),
                },
            ),
        ))
    }
}

#[derive(Debug, Clone, TreeParser, TreeSyntax, TreeText)]
pub enum SetOperator {
    Union(Union),
    Except(Except),
    Minus(Minus),
    Intersect(Intersect),
}

#[derive(Debug, Clone, TreeParser, TreeSyntax, TreeText)]
pub enum SetQuantifier {
    Distinct(Distinct),
    DistinctByName(Distinct, By, Name),
    All(All),
    AllByName(All, By, Name),
    ByName(By, Name),
}

#[derive(Debug, Clone, TreeParser, TreeSyntax, TreeText)]
#[parser(dependency = "(Query, Expr, TableWithJoins)")]
pub enum QueryTerm {
    Select(#[parser(function = |(q, e, t), o| boxed(compose((q, e, t), o)))] Box<QuerySelect>),
    Table(Table, ObjectName),
    Values(#[parser(function = |(_, e, _), o| compose(e, o))] ValuesClause),
    Nested(
        LeftParenthesis,
        #[parser(function = |(q, _, _), _| q)] Query,
        RightParenthesis,
    ),
    // HiveQL FROM-first syntax: FROM <tables> SELECT ... (equivalent to SELECT ... FROM <tables>)
    HiveFrom(#[parser(function = |(q, e, t), o| boxed(compose((q, e, t), o)))] Box<HiveFromTerm>),
}

/// HiveQL FROM-first query: `FROM <tables> [LATERAL VIEW ...] (SELECT ... [WHERE ...] ...)*`
#[derive(Debug, Clone, TreeParser, TreeSyntax, TreeText)]
#[parser(dependency = "(Query, Expr, TableWithJoins)")]
pub struct HiveFromTerm {
    pub from_kw: From,
    #[parser(function = |(_, _, t), o| sequence(t, unit(o)))]
    pub tables: Sequence<TableWithJoins, Comma>,
    #[parser(function = |(_, e, _), o| compose(e, o))]
    pub lateral_views: Vec<LateralViewClause>,
    #[parser(function = |(_, e, _), o| compose(e, o))]
    pub bodies: Vec<HiveFromBody>,
}

/// A single body inside a HiveQL FROM-first query (SELECT or INSERT INTO ... SELECT).
#[derive(Debug, Clone, TreeParser, TreeSyntax, TreeText)]
#[parser(dependency = "Expr")]
pub enum HiveFromBody {
    Select {
        #[parser(function = |e, o| compose(e, o))]
        select: SelectClause,
        #[parser(function = |e, o| compose(e, o))]
        lateral_views: Vec<LateralViewClause>,
        #[parser(function = |e, o| compose(e, o))]
        r#where: Option<WhereClause>,
        #[parser(function = |e, o| compose(e, o))]
        group_by: Option<GroupByClause>,
        #[parser(function = |e, o| compose(e, o))]
        having: Option<HavingClause>,
        #[parser(function = |e, o| compose(e, o))]
        qualify: Option<QualifyClause>,
    },
    Insert {
        insert: Insert,
        into: IntoKeyword,
        table: Option<Table>,
        #[parser(function = |_, o| object_name(table_ident(o), o))]
        name: ObjectName,
        #[parser(function = |e, o| compose(e, o))]
        select: SelectClause,
        #[parser(function = |e, o| compose(e, o))]
        lateral_views: Vec<LateralViewClause>,
        #[parser(function = |e, o| compose(e, o))]
        r#where: Option<WhereClause>,
        #[parser(function = |e, o| compose(e, o))]
        group_by: Option<GroupByClause>,
        #[parser(function = |e, o| compose(e, o))]
        having: Option<HavingClause>,
        #[parser(function = |e, o| compose(e, o))]
        qualify: Option<QualifyClause>,
        #[parser(function = |e, o| compose(e, o))]
        limit: Option<LimitClause>,
    },
}

#[derive(Debug, Clone, TreeParser, TreeSyntax, TreeText)]
#[parser(dependency = "(Query, Expr, TableWithJoins)")]
pub struct QuerySelect {
    #[parser(function = |(_, e, _), o| compose(e, o))]
    pub select: SelectClause,
    #[parser(function = |(_, _, t), o| compose(t, o))]
    pub from: Option<FromClause>,
    #[parser(function = |(_, e, _), o| compose(e, o))]
    pub lateral_views: Vec<LateralViewClause>,
    #[parser(function = |(_, e, _), o| compose(e, o))]
    pub r#where: Option<WhereClause>,
    #[parser(function = |(_, e, _), o| compose(e, o))]
    pub group_by: Option<GroupByClause>,
    #[parser(function = |(_, e, _), o| compose(e, o))]
    pub having: Option<HavingClause>,
    #[parser(function = |(_, e, _), o| compose(e, o))]
    pub qualify: Option<QualifyClause>,
}

#[derive(Debug, Clone, TreeParser, TreeSyntax, TreeText)]
#[parser(dependency = "Expr")]
pub struct ValuesClause {
    pub values: Values,
    #[parser(function = |e, o| sequence(e, unit(o)))]
    pub expressions: Sequence<Expr, Comma>,
    pub alias: Option<AliasClause>,
}

#[derive(Debug, Clone, TreeParser, TreeSyntax, TreeText)]
pub struct AliasClause {
    pub r#as: Option<As>,
    #[parser(function = |(), o| table_ident(o))]
    pub table: Ident,
    pub columns: Option<IdentList>,
}

#[derive(Debug, Clone, TreeParser, TreeSyntax, TreeText)]
#[parser(dependency = "Expr")]
pub struct SelectClause {
    pub select: Select,
    pub quantifier: Option<DuplicateTreatment>,
    #[parser(function = |e, o| sequence(compose((e, column_ident(o)), o), unit(o)))]
    pub projection: Sequence<NamedExpr, Comma>,
}

#[derive(Debug, Clone, TreeParser, TreeSyntax, TreeText)]
#[parser(dependency = "(Expr, Ident)")]
pub struct NamedExpr {
    #[parser(function = |(e, _), _| e)]
    pub expr: Expr,
    // If the alias is an identifier list, it will be parsed by the default `Ident` parser
    // rather than the restricted `Ident` parser passed as a dependency.
    // This is because the identifier list is inside the parentheses so there will be no ambiguity.
    #[parser(function = |(_, i), o| unit(o).or_not().then(either_or(i, unit(o))).or_not())]
    pub alias: Option<(Option<As>, Either<Ident, IdentList>)>,
}

#[derive(Debug, Clone, TreeParser, TreeSyntax, TreeText)]
#[parser(dependency = "Expr")]
pub struct NamedExprList {
    pub left: LeftParenthesis,
    // We do not need to restrict the alias identifier since the named expression
    // is inside the parentheses so there will be no ambiguity even if `AS` is left out.
    #[parser(function = |e, o| sequence(compose((e, unit(o)), o), unit(o)))]
    pub items: Sequence<NamedExpr, Comma>,
    pub right: RightParenthesis,
}

#[derive(Debug, Clone, TreeParser, TreeSyntax, TreeText)]
#[parser(dependency = "TableWithJoins")]
pub struct FromClause {
    pub from: From,
    #[parser(function = |t, o| sequence(t, unit(o)))]
    pub tables: Sequence<TableWithJoins, Comma>,
}

#[derive(Debug, Clone, TreeParser, TreeSyntax, TreeText)]
#[parser(dependency = "(Query, Expr, TableWithJoins)")]
pub struct TableWithJoins {
    pub lateral: Option<Lateral>,
    #[parser(function = |(q, e, t), o| compose((q, e, t), o))]
    pub table: TableFactor,
    #[parser(function = |(q, e, t), o| compose((q, e, t), o))]
    pub joins: Vec<TableJoin>,
}

#[derive(Debug, Clone, TreeParser, TreeSyntax, TreeText)]
#[parser(dependency = "(Query, Expr, TableWithJoins)")]
pub enum TableFactor {
    Values {
        #[parser(function = |(_, e, _), o| compose(e, o))]
        values: ValuesClause,
        alias: Option<AliasClause>,
    },
    Query {
        left: LeftParenthesis,
        #[parser(function = |(q, _, _), _| q)]
        query: Query,
        right: RightParenthesis,
        #[parser(function = |(_, e, _), o| boxed(compose(e, o)).or_not())]
        sample: Option<Box<TableSampleClause>>,
        #[parser(function = |(_, e, _), o| compose(e, o))]
        modifiers: Vec<TableModifier>,
        alias: Option<AliasClause>,
    },
    Nested {
        left: LeftParenthesis,
        #[parser(function = |(_, _, t), _| boxed(t))]
        table: Box<TableWithJoins>,
        right: RightParenthesis,
        #[parser(function = |(_, e, _), o| compose(e, o))]
        modifiers: Vec<TableModifier>,
        alias: Option<AliasClause>,
    },
    Identifier {
        identifier: Identifier,
        left: LeftParenthesis,
        #[parser(function = |(_, e, _), _| e)]
        expr: Expr,
        right: RightParenthesis,
        #[parser(function = |(_, e, _), o| compose(e, o))]
        modifiers: Vec<TableModifier>,
        alias: Option<AliasClause>,
    },
    TableFunction {
        #[parser(function = |(_, e, _), o| compose(e, o))]
        function: TableFunction,
        #[parser(function = |(_, e, _), o| boxed(compose(e, o)).or_not())]
        sample: Option<Box<TableSampleClause>>,
        alias: Option<AliasClause>,
    },
    Name {
        name: ObjectName,
        #[parser(function = |(_, e, _), o| boxed(compose(e, o)).or_not())]
        temporal: Option<Box<TemporalClause>>,
        #[parser(function = |(_, e, _), o| boxed(compose(e, o)).or_not())]
        sample: Option<Box<TableSampleClause>>,
        #[parser(function = |(_, e, _), o| compose(e, o))]
        modifiers: Vec<TableModifier>,
        alias: Option<AliasClause>,
    },
}

#[derive(Debug, Clone, TreeParser, TreeSyntax, TreeText)]
#[parser(dependency = "Expr")]
pub enum TemporalClause {
    Version {
        r#for: Option<For>,
        version: Either<SystemVersion, Version>,
        as_of: Option<(As, Of)>,
        #[parser(function = |e, _| e)]
        value: Expr,
    },
    Timestamp {
        r#for: Option<For>,
        timestamp: Either<SystemTime, Timestamp>,
        as_of: Option<(As, Of)>,
        #[parser(function = |e, _| e)]
        value: Expr,
    },
}

#[derive(Debug, Clone, TreeParser, TreeSyntax, TreeText)]
#[parser(dependency = "Expr")]
pub struct TableSampleClause {
    pub sample: Tablesample,
    pub left: LeftParenthesis,
    #[parser(function = |e, o| compose(e, o))]
    pub method: TableSampleMethod,
    pub right: RightParenthesis,
    pub repeatable: Option<TableSampleRepeatable>,
}

#[derive(Debug, Clone, TreeParser, TreeSyntax, TreeText)]
#[parser(dependency = "Expr")]
pub enum TableSampleMethod {
    Percent {
        #[parser(function = |e, _| e)]
        value: Expr,
        percent: Percent,
    },
    Rows {
        #[parser(function = |e, _| e)]
        value: Expr,
        rows: Rows,
    },
    Buckets {
        bucket: Bucket,
        numerator: IntegerLiteral,
        out_of: (Out, Of),
        denominator: IntegerLiteral,
        #[parser(function = |e, o| unit(o).then(e).or_not())]
        on_expr: Option<(On, Expr)>,
    },
    // Size-based: TABLESAMPLE(300M), TABLESAMPLE(1G), etc.
    ByteSize {
        #[parser(function = |e, _| e)]
        value: Expr,
        // M, K, G, T, P, B — parsed as an identifier since they are not keywords
        unit: Ident,
    },
}

#[derive(Debug, Clone, TreeParser, TreeSyntax, TreeText)]
pub struct TableSampleRepeatable {
    pub repeatable: Repeatable,
    pub left: LeftParenthesis,
    pub seed: IntegerLiteral,
    pub right: RightParenthesis,
}

#[derive(Debug, Clone, TreeParser, TreeSyntax, TreeText)]
#[parser(dependency = "Expr")]
#[expect(clippy::large_enum_variant)]
pub enum TableModifier {
    Pivot(#[parser(function = |e, o| compose(e, o))] PivotClause),
    Unpivot(UnpivotClause),
    Watermark(#[parser(function = |e, o| compose(e, o))] WatermarkModifier),
}

#[derive(Debug, Clone, TreeParser, TreeSyntax, TreeText)]
#[parser(dependency = "Expr")]
pub struct WatermarkModifier {
    pub watermark: Watermark,
    #[parser(function = |e, _| e)]
    pub event_time: Expr,
    pub event_time_alias: Option<(As, Ident)>,
    pub delay: Delay,
    pub of: Of,
    #[parser(function = |e, _| e)]
    pub interval_expr: Expr,
}

#[derive(Debug, Clone, TreeParser, TreeSyntax, TreeText)]
#[parser(dependency = "Expr")]
pub struct PivotClause {
    pub pivot: Pivot,
    pub left: LeftParenthesis,
    #[parser(function = |e, o| sequence(compose((e, column_ident(o)), o), unit(o)))]
    pub aggregates: Sequence<NamedExpr, Comma>,
    pub r#for: For,
    pub columns: Either<Ident, IdentList>,
    pub r#in: In,
    #[parser(function = |e, o| compose(e, o))]
    pub values: NamedExprList,
    pub right: RightParenthesis,
}

#[derive(Debug, Clone, TreeParser, TreeSyntax, TreeText)]
pub struct UnpivotClause {
    pub unpivot: Unpivot,
    pub nulls: Option<UnpivotNulls>,
    pub left: LeftParenthesis,
    pub columns: UnpivotColumns,
    pub right: RightParenthesis,
}

#[derive(Debug, Clone, TreeParser, TreeSyntax, TreeText)]
pub enum UnpivotNulls {
    IncludeNulls(Include, Nulls),
    ExcludeNulls(Exclude, Nulls),
}

#[derive(Debug, Clone, TreeParser, TreeSyntax, TreeText)]
pub enum UnpivotColumns {
    SingleValue {
        values: Ident,
        r#for: For,
        name: Ident,
        r#in: In,
        left: LeftParenthesis,
        // Allow empty IN list ()
        #[expect(clippy::type_complexity)]
        columns: Option<Sequence<(Ident, Option<(Option<As>, Ident)>), Comma>>,
        right: RightParenthesis,
    },
    MultiValue {
        // Allow empty value tuple () or (v1, v2, ...)
        values: UnpivotValueSpec,
        r#for: For,
        name: Ident,
        r#in: In,
        left: LeftParenthesis,
        // Allow empty IN list ()
        columns: Option<Sequence<UnpivotColumnGroup, Comma>>,
        right: RightParenthesis,
    },
}

/// The value specification for multi-column UNPIVOT: `()` or `(val1, val2, ...)`.
#[derive(Debug, Clone, TreeParser, TreeSyntax, TreeText)]
pub enum UnpivotValueSpec {
    Empty(LeftParenthesis, RightParenthesis),
    NonEmpty(IdentList),
}

/// A column group in the UNPIVOT IN list: `(col1, col2 AS alias, ...)` with optional outer alias.
#[derive(Debug, Clone, TreeParser, TreeSyntax, TreeText)]
pub struct UnpivotColumnGroup {
    pub left: LeftParenthesis,
    // Allow empty for (())
    pub names: Option<Sequence<UnpivotColumnItem, Comma>>,
    pub right: RightParenthesis,
    pub alias: Option<(Option<As>, Ident)>,
}

/// A column reference inside an UNPIVOT group, with optional alias: `col_name [AS alias]`.
#[derive(Debug, Clone, TreeParser, TreeSyntax, TreeText)]
pub struct UnpivotColumnItem {
    pub name: Ident,
    pub alias: Option<(Option<As>, Ident)>,
}

#[derive(Debug, Clone, TreeParser, TreeSyntax, TreeText)]
#[parser(dependency = "Expr")]
pub struct TableFunction {
    pub name: ObjectName,
    pub left: LeftParenthesis,
    #[parser(function = |e, o| sequence(compose(e, o), unit(o)).or_not())]
    pub arguments: Option<Sequence<FunctionArgument, Comma>>,
    pub right: RightParenthesis,
}

#[derive(Debug, Clone, TreeParser, TreeSyntax, TreeText)]
#[parser(dependency = "(Query, Expr, TableWithJoins)")]
pub struct TableJoin {
    // The join criteria must be absent for natural joins.
    // But we defer the enforcement of this to later stages of SQL analysis.
    pub natural: Option<Natural>,
    pub operator: Option<JoinOperator>,
    pub join: Join,
    pub lateral: Option<Lateral>,
    #[parser(function = |(q, e, t), o| compose((q, e, t), o))]
    pub other: TableFactor,
    #[parser(function = |(_, e, _), o| compose(e, o))]
    pub criteria: Option<JoinCriteria>,
}

#[derive(Debug, Clone, TreeParser, TreeSyntax, TreeText)]
pub enum JoinOperator {
    Inner(Inner),
    Cross(Cross),
    Outer(Outer),
    Semi(Semi),
    Anti(Anti),
    LeftOuter(Left, Outer),
    LeftSemi(Left, Semi),
    LeftAnti(Left, Anti),
    Left(Left),
    RightOuter(Right, Outer),
    RightSemi(Right, Semi),
    RightAnti(Right, Anti),
    Right(Right),
    FullOuter(Full, Outer),
    Full(Full),
}

#[derive(Debug, Clone, TreeParser, TreeSyntax, TreeText)]
#[parser(dependency = "Expr")]
pub enum JoinCriteria {
    On(On, #[parser(function = |e, _| e)] Expr),
    Using(Using, IdentList),
}

#[derive(Debug, Clone, TreeParser, TreeSyntax, TreeText)]
#[parser(dependency = "Expr")]
pub struct LateralViewClause {
    pub lateral_view: (Lateral, View),
    pub outer: Option<Outer>,
    pub function: ObjectName,
    pub left: LeftParenthesis,
    #[parser(function = |e, o| sequence(compose(e, o), unit(o)).or_not())]
    pub arguments: Option<Sequence<FunctionArgument, Comma>>,
    pub right: RightParenthesis,
    // FIXME: When both the table alias and the `AS` keyword are omitted,
    //   the column aliases cannot be parsed correctly.
    #[parser(function = |_, o| object_name(table_ident(o), o).or_not())]
    pub table: Option<ObjectName>,
    #[parser(function = |_, o| unit(o).then(sequence(column_ident(o), unit(o))).or_not())]
    pub columns: Option<(Option<As>, Sequence<Ident, Comma>)>,
}

#[derive(Debug, Clone, TreeParser, TreeSyntax, TreeText)]
#[parser(dependency = "Expr")]
pub struct WhereClause {
    pub r#where: Where,
    #[parser(function = |e, _| e)]
    pub condition: Expr,
}

#[derive(Debug, Clone, TreeParser, TreeSyntax, TreeText)]
#[parser(dependency = "Expr")]
pub struct GroupByClause {
    pub group_by: (Group, By),
    #[parser(function = |e, o| sequence(compose(e, o), unit(o)))]
    pub expressions: Sequence<GroupingExpr, Comma>,
    #[parser(function = |e, o| compose(e, o).or_not())]
    pub modifier: Option<GroupByModifier>,
}

#[derive(Debug, Clone, TreeParser, TreeSyntax, TreeText)]
#[parser(dependency = "Expr")]
pub enum GroupByModifier {
    WithRollup(With, Rollup),
    WithCube(With, Cube),
    // GROUPING SETS (...) as a trailing modifier after GROUP BY a, b
    GroupingSets(
        Grouping,
        Sets,
        LeftParenthesis,
        #[parser(function = |e, o| sequence(compose(e, o), unit(o)))] Sequence<GroupingSet, Comma>,
        RightParenthesis,
    ),
}

#[derive(Debug, Clone, TreeParser, TreeSyntax, TreeText)]
#[parser(dependency = "Expr")]
pub struct HavingClause {
    pub having: Having,
    #[parser(function = |e, _| e)]
    pub condition: Expr,
}

#[derive(Debug, Clone, TreeParser, TreeSyntax, TreeText)]
#[parser(dependency = "Expr")]
pub struct QualifyClause {
    pub qualify: Qualify,
    #[parser(function = |e, _| e)]
    pub condition: Expr,
}

#[derive(Debug, Clone, TreeParser, TreeSyntax, TreeText)]
#[parser(dependency = "Expr")]
pub struct WindowClause {
    pub window: Window,
    #[parser(function = |e, o| sequence(compose(e, o), unit(o)))]
    pub items: Sequence<NamedWindow, Comma>,
}

#[derive(Debug, Clone, TreeParser, TreeSyntax, TreeText)]
#[parser(dependency = "Expr")]
pub struct NamedWindow {
    pub name: Ident,
    pub r#as: As,
    #[parser(function = |e, o| compose(e, o))]
    pub window: WindowSpec,
}

#[derive(Debug, Clone, TreeParser, TreeSyntax, TreeText)]
#[parser(dependency = "Expr")]
pub struct OrderByClause {
    pub order_by: (Order, By),
    #[parser(function = |e, o| sequence(compose(e, o), unit(o)))]
    pub items: Sequence<OrderByExpr, Comma>,
}

#[derive(Debug, Clone, TreeParser, TreeSyntax, TreeText)]
#[parser(dependency = "Expr")]
pub struct SortByClause {
    pub sort_by: (Sort, By),
    #[parser(function = |e, o| sequence(compose(e, o), unit(o)))]
    pub items: Sequence<OrderByExpr, Comma>,
}

#[derive(Debug, Clone, TreeParser, TreeSyntax, TreeText)]
#[parser(dependency = "Expr")]
pub struct ClusterByClause {
    pub cluster_by: (Cluster, By),
    #[parser(function = |e, o| sequence(e, unit(o)))]
    pub items: Sequence<Expr, Comma>,
}

#[derive(Debug, Clone, TreeParser, TreeSyntax, TreeText)]
#[parser(dependency = "Expr")]
pub struct PartitionByClause {
    pub partition_by: (Partition, By),
    #[parser(function = |e, o| sequence(e, unit(o)))]
    pub items: Sequence<Expr, Comma>,
}

#[derive(Debug, Clone, TreeParser, TreeSyntax, TreeText)]
#[parser(dependency = "Expr")]
pub struct DistributeByClause {
    pub distribute_by: (Distribute, By),
    #[parser(function = |e, o| sequence(e, unit(o)))]
    pub items: Sequence<Expr, Comma>,
}

#[derive(Debug, Clone, TreeParser, TreeSyntax, TreeText)]
#[parser(dependency = "Expr")]
pub struct LimitClause {
    pub limit: Limit,
    #[parser(function = |e, o| compose(e, o))]
    pub value: LimitValue,
}

#[derive(Debug, Clone, TreeParser, TreeSyntax, TreeText)]
#[parser(dependency = "Expr")]
pub enum LimitValue {
    All(All),
    Value(#[parser(function = |e, _| boxed(e))] Box<Expr>),
}

#[derive(Debug, Clone, TreeParser, TreeSyntax, TreeText)]
#[parser(dependency = "Expr")]
pub struct OffsetClause {
    pub offset: Offset,
    #[parser(function = |e, _| e)]
    pub value: Expr,
}
