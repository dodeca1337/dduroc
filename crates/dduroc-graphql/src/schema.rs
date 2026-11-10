//! GraphQL roots and request-wide admission/projection budgets.
use crate::{
    Service, ServiceError,
    input::{CatalogInput, LimitsInput, PageInput, QueryInput},
    types::{self, CatalogView, FollowBatchView, PageView, Presentation, UInt64},
};
use async_graphql::extensions::{
    Extension, ExtensionContext, ExtensionFactory, NextExecute, NextParseQuery, NextPrepareRequest,
    NextResolve, NextSubscribe, ResolveInfo,
};
use async_graphql::{
    Context, Error, ErrorExtensions, InputObject, Object, Request, Response, Schema, ServerError,
    ServerResult, SimpleObject, Subscription, Value, Variables,
};
use async_graphql_parser::types::{ExecutableDocument, Selection, SelectionSet};
use futures_core::Stream;
use futures_util::{
    StreamExt,
    future::{Either, select},
    stream::BoxStream,
};
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicBool, Ordering},
};
use tokio::sync::{Notify, Semaphore};

/// Host-owned ceilings for the complete GraphQL operation. HTTP body limits
/// remain the transport host's responsibility before constructing Request.
#[derive(Debug, Clone)]
pub struct SchemaLimits {
    pub max_document_bytes: usize,
    pub max_variable_bytes: usize,
    pub max_variable_nodes: usize,
    pub max_variable_depth: usize,
    pub max_selections: usize,
    pub max_aliases: usize,
    pub max_alias_bytes: usize,
    pub max_depth: usize,
    pub max_complexity: usize,
    pub max_root_calls: usize,
    pub max_conversion_bytes: usize,
    pub max_response_bytes: usize,
    pub max_resolved_fields: usize,
    pub max_errors: usize,
    pub max_error_bytes: usize,
}
impl Default for SchemaLimits {
    fn default() -> Self {
        Self {
            max_document_bytes: 64 * 1024,
            max_variable_bytes: 64 * 1024,
            max_variable_nodes: 4096,
            max_variable_depth: 16,
            max_selections: 2048,
            max_aliases: 64,
            max_alias_bytes: 64,
            max_depth: 16,
            max_complexity: 10_000,
            max_root_calls: 8,
            max_conversion_bytes: 16 * 1024 * 1024,
            max_response_bytes: 16 * 1024 * 1024,
            max_resolved_fields: 100_000,
            max_errors: 16,
            max_error_bytes: 1024,
        }
    }
}
#[derive(Debug, Default)]
struct Counts {
    roots: usize,
    conversion: usize,
    response: usize,
    fields: usize,
}
struct RequestBudget {
    limits: SchemaLimits,
    counts: Mutex<Counts>,
    failed: AtomicBool,
    stopped: Notify,
    leaf_gate: Semaphore,
    touched: Mutex<Vec<(Service, String)>>,
}
impl RequestBudget {
    fn new(limits: SchemaLimits) -> Self {
        Self {
            limits,
            counts: Mutex::new(Counts::default()),
            failed: AtomicBool::new(false),
            stopped: Notify::new(),
            leaf_gate: Semaphore::new(1),
            touched: Mutex::new(Vec::new()),
        }
    }
    fn fail(&self) -> ServerError {
        self.failed.store(true, Ordering::Release);
        self.stopped.notify_one();
        budget_error()
    }
    fn check(&self) -> ServerResult<()> {
        if self.failed.load(Ordering::Acquire) {
            Err(budget_error())
        } else {
            Ok(())
        }
    }
    fn root(&self, conversion: usize) -> ServerResult<()> {
        self.check()?;
        let mut c = self.counts.lock().unwrap();
        c.roots = c.roots.saturating_add(1);
        c.conversion = c.conversion.saturating_add(conversion);
        if c.roots > self.limits.max_root_calls || c.conversion > self.limits.max_conversion_bytes {
            return Err(self.fail());
        }
        Ok(())
    }
    fn field(&self, name: &str) -> ServerResult<()> {
        self.check()?;
        let mut c = self.counts.lock().unwrap();
        c.fields = c.fields.saturating_add(1);
        c.response = c
            .response
            .saturating_add(name.len().saturating_mul(6).saturating_add(16));
        if c.fields > self.limits.max_resolved_fields || c.response > self.limits.max_response_bytes
        {
            return Err(self.fail());
        }
        Ok(())
    }
    fn value(&self, value: &Value) -> ServerResult<()> {
        fn cost(value: &Value) -> usize {
            match value {
                Value::String(s) => s.len().saturating_mul(6).saturating_add(2),
                Value::Binary(bytes) => bytes.len().saturating_mul(8).saturating_add(2),
                Value::List(items) => items
                    .iter()
                    .fold(2usize, |n, v| n.saturating_add(cost(v)).saturating_add(1)),
                Value::Object(fields) => fields.iter().fold(2usize, |n, (k, v)| {
                    n.saturating_add(k.len().saturating_mul(6))
                        .saturating_add(cost(v))
                        .saturating_add(4)
                }),
                _ => 64,
            }
        }
        let mut c = self.counts.lock().unwrap();
        c.response = c.response.saturating_add(cost(value));
        if c.response > self.limits.max_response_bytes {
            return Err(self.fail());
        }
        Ok(())
    }
}
struct CapabilityGuard {
    budget: Arc<RequestBudget>,
    delivered: bool,
}
impl Drop for CapabilityGuard {
    fn drop(&mut self) {
        let touched = std::mem::take(&mut *self.budget.touched.lock().unwrap());
        if !self.delivered {
            for (service, id) in touched {
                let _ = service.cancel_now(&id);
            }
        }
    }
}
fn track(ctx: &Context<'_>, service: &Service, id: &str) -> async_graphql::Result<()> {
    ctx.data::<Arc<RequestBudget>>()?
        .touched
        .lock()
        .unwrap()
        .push((service.clone(), id.to_owned()));
    Ok(())
}
fn budget_error() -> ServerError {
    let mut error = Error::new("GraphQL operation budget exceeded")
        .extend_with(|_, extensions| extensions.set("code", "OPERATION_LIMIT_EXCEEDED"))
        .into_server_error(async_graphql::Pos::default());
    error.locations.clear();
    error
}
struct LimitsExtension {
    limits: SchemaLimits,
    budget: Arc<RequestBudget>,
}
#[derive(Clone)]
struct BudgetFactory(SchemaLimits);
impl ExtensionFactory for BudgetFactory {
    fn create(&self) -> Arc<dyn Extension> {
        Arc::new(LimitsExtension {
            limits: self.0.clone(),
            budget: Arc::new(RequestBudget::new(self.0.clone())),
        })
    }
}
fn cap_errors(mut response: Response, limits: &SchemaLimits) -> Response {
    let truncated = response.errors.len() > limits.max_errors;
    response.errors.truncate(limits.max_errors);
    for error in &mut response.errors {
        error.message = crate::error::bounded_message(&error.message, limits.max_error_bytes);
        if error.extensions.is_none() {
            error.extensions = Some(Default::default());
            error
                .extensions
                .as_mut()
                .unwrap()
                .set("code", "GRAPHQL_ERROR");
        }
    }
    if truncated && let Some(error) = response.errors.first_mut() {
        error
            .extensions
            .get_or_insert_with(Default::default)
            .set("errorsTruncated", true);
    }
    response
}
#[async_graphql::async_trait::async_trait]
impl Extension for LimitsExtension {
    async fn request(
        &self,
        ctx: &ExtensionContext<'_>,
        next: async_graphql::extensions::NextRequest<'_>,
    ) -> Response {
        cap_errors(next.run(ctx).await, &self.limits)
    }

    async fn prepare_request(
        &self,
        ctx: &ExtensionContext<'_>,
        request: Request,
        next: NextPrepareRequest<'_>,
    ) -> ServerResult<Request> {
        if request.query.len() > self.limits.max_document_bytes {
            return Err(budget_error());
        }
        fn visit(
            value: &Value,
            depth: usize,
            nodes: &mut usize,
            bytes: &mut usize,
            limits: &SchemaLimits,
        ) -> bool {
            *nodes = nodes.saturating_add(1);
            if depth > limits.max_variable_depth || *nodes > limits.max_variable_nodes {
                return false;
            }
            *bytes = bytes.saturating_add(16);
            match value {
                Value::String(text) => *bytes = bytes.saturating_add(text.len().saturating_mul(6)),
                Value::Binary(value) => *bytes = bytes.saturating_add(value.len()),
                Value::List(items) => {
                    for item in items {
                        if !visit(item, depth + 1, nodes, bytes, limits) {
                            return false;
                        }
                    }
                }
                Value::Object(items) => {
                    for (key, value) in items {
                        *bytes = bytes.saturating_add(key.len().saturating_mul(6));
                        if !visit(value, depth + 1, nodes, bytes, limits) {
                            return false;
                        }
                    }
                }
                _ => {}
            }
            *bytes <= limits.max_variable_bytes
        }
        let (mut nodes, mut bytes) = (0usize, 0usize);
        for (key, value) in request.variables.iter() {
            bytes = bytes.saturating_add(key.len());
            if !visit(value, 0, &mut nodes, &mut bytes, &self.limits) {
                return Err(budget_error());
            }
        }
        next.run(ctx, request.data(Arc::clone(&self.budget))).await
    }
    async fn parse_query(
        &self,
        ctx: &ExtensionContext<'_>,
        query: &str,
        variables: &Variables,
        next: NextParseQuery<'_>,
    ) -> ServerResult<ExecutableDocument> {
        let document = next.run(ctx, query, variables).await?;
        fn walk(
            set: &SelectionSet,
            document: &ExecutableDocument,
            depth: usize,
            count: &mut usize,
            aliases: &mut usize,
            limits: &SchemaLimits,
        ) -> ServerResult<()> {
            if depth > limits.max_depth {
                return Err(budget_error());
            }
            for selection in &set.items {
                *count = count.saturating_add(1);
                if *count > limits.max_selections {
                    return Err(budget_error());
                }
                match &selection.node {
                    Selection::Field(field) => {
                        if let Some(alias) = &field.node.alias {
                            *aliases = aliases.saturating_add(1);
                            if alias.node.len() > limits.max_alias_bytes
                                || *aliases > limits.max_aliases
                            {
                                return Err(budget_error());
                            }
                        }
                        walk(
                            &field.node.selection_set.node,
                            document,
                            depth + 1,
                            count,
                            aliases,
                            limits,
                        )?;
                    }
                    Selection::InlineFragment(fragment) => walk(
                        &fragment.node.selection_set.node,
                        document,
                        depth + 1,
                        count,
                        aliases,
                        limits,
                    )?,
                    Selection::FragmentSpread(spread) => {
                        if let Some(fragment) =
                            document.fragments.get(&spread.node.fragment_name.node)
                        {
                            walk(
                                &fragment.node.selection_set.node,
                                document,
                                depth + 1,
                                count,
                                aliases,
                                limits,
                            )?;
                        }
                    }
                }
            }
            Ok(())
        }
        let (mut count, mut aliases) = (0, 0);
        for (_, operation) in document.operations.iter() {
            walk(
                &operation.node.selection_set.node,
                &document,
                0,
                &mut count,
                &mut aliases,
                &self.limits,
            )?;
        }
        Ok(document)
    }
    async fn execute(
        &self,
        ctx: &ExtensionContext<'_>,
        name: Option<&str>,
        next: NextExecute<'_>,
    ) -> Response {
        let Some(budget) = ctx.data_opt::<Arc<RequestBudget>>().cloned() else {
            return Response::from_errors(vec![budget_error()]);
        };
        let mut capabilities = CapabilityGuard {
            budget: Arc::clone(&budget),
            delivered: false,
        };
        let running = next.run(ctx, name);
        let stopped = budget.stopped.notified();
        futures_util::pin_mut!(running, stopped);
        let response = match select(running, stopped).await {
            Either::Left((response, _)) if !budget.failed.load(Ordering::Acquire) => response,
            _ => Response::from_errors(vec![budget_error()]),
        };
        capabilities.delivered = !matches!(response.data, Value::Null);
        response
    }
    async fn resolve(
        &self,
        ctx: &ExtensionContext<'_>,
        info: ResolveInfo<'_>,
        next: NextResolve<'_>,
    ) -> ServerResult<Option<Value>> {
        let budget = ctx
            .data_opt::<Arc<RequestBudget>>()
            .ok_or_else(budget_error)?;
        budget.field(info.alias.unwrap_or(info.name))?;
        let return_type = info.return_type.trim_end_matches('!');
        let leaf = matches!(
            return_type,
            "String" | "Int" | "Float" | "Boolean" | "ID" | "UInt64" | "Int64"
        );
        // Only one potentially large scalar projection is copied at once. On
        // exhaustion execute drops the complete execution future, including
        // pending aliases; worker cancellation retains native permits.
        let _lease = if leaf {
            Some(
                budget
                    .leaf_gate
                    .acquire()
                    .await
                    .map_err(|_| budget_error())?,
            )
        } else {
            None
        };
        budget.check()?;
        let result = next.run(ctx, info).await?;
        if (leaf || matches!(result, Some(Value::Enum(_))))
            && let Some(value) = &result
        {
            budget.value(value)?;
        }
        Ok(result)
    }
    fn subscribe<'s>(
        &self,
        ctx: &ExtensionContext<'_>,
        stream: BoxStream<'s, Response>,
        next: NextSubscribe<'_>,
    ) -> BoxStream<'s, Response> {
        // subscribe is called before prepare_request initializes query_data.
        let budget = Some(Arc::clone(&self.budget));
        let limits = self.limits.clone();
        let stream = next
            .run(ctx, stream)
            .map(move |response| cap_errors(response, &limits))
            .boxed();
        Box::pin(futures_util::stream::unfold(
            (stream, budget, false),
            |(mut stream, budget, ended)| async move {
                if ended {
                    return None;
                }
                let Some(budget) = budget else {
                    return Some((
                        Response::from_errors(vec![budget_error()]),
                        (stream, None, true),
                    ));
                };
                if budget.failed.load(Ordering::Acquire) {
                    return Some((
                        Response::from_errors(vec![budget_error()]),
                        (stream, Some(budget), true),
                    ));
                }
                let item = {
                    let running = stream.next();
                    let stopped = budget.stopped.notified();
                    futures_util::pin_mut!(running, stopped);
                    match select(running, stopped).await {
                        Either::Left((item, _)) => item,
                        Either::Right(_) => Some(Response::from_errors(vec![budget_error()])),
                    }
                };
                item.map(|response| {
                    let ended = budget.failed.load(Ordering::Acquire);
                    (response, (stream, Some(budget), ended))
                })
            },
        ))
    }
}

#[derive(Debug, Clone, Default, InputObject)]
pub struct PresentationInput {
    pub language: Option<String>,
    pub languages: Option<Vec<String>>,
}
impl PresentationInput {
    fn native(
        &self,
        defaults: &dduroc_read::LanguagePreferences,
    ) -> Result<Presentation, ServiceError> {
        if self.language.is_some() && self.languages.is_some() {
            return Err(ServiceError::InvalidInput(
                "language and languages are mutually exclusive".into(),
            ));
        }
        let preferences = if let Some(codes) = &self.languages {
            dduroc_read::LanguagePreferences::new(codes.clone())
        } else if let Some(code) = &self.language {
            dduroc_read::LanguagePreferences::new([code.clone()])
        } else {
            Ok(defaults.clone())
        }
        .map_err(|_| ServiceError::InvalidInput("invalid language preferences".into()))?;
        Ok(Presentation {
            languages: Some(preferences),
            ..Presentation::default()
        })
    }
}
#[derive(Debug, Clone, SimpleObject)]
pub struct ReadSessionOpened {
    pub id: String,
    pub revision: UInt64,
}
#[derive(Debug, Clone, SimpleObject)]
pub struct CountedReadSessionOpened {
    pub id: String,
    pub revision: UInt64,
    pub total_matches: UInt64,
    pub total_seeds: UInt64,
    pub total_pages: UInt64,
}
#[derive(Debug, Clone, SimpleObject)]
pub struct ReadSessionPage {
    pub revision: UInt64,
    pub page: PageView,
}
#[derive(Debug, Clone, SimpleObject)]
pub struct ReadSessionSeekPage {
    pub revision: UInt64,
    pub reached_page: UInt64,
    pub target_reached: bool,
    pub exhausted: bool,
    pub page: Option<PageView>,
}
fn projection(ctx: &Context<'_>, wrapped: bool) -> types::Projection {
    let look = ctx.look_ahead();
    let page = if wrapped { look.field("page") } else { look };
    let entries = [page.field("entries"), page.field("seeds")];
    types::Projection {
        fields: entries.iter().any(|entry| {
            entry.field("message").field("fields").exists()
                || entry.field("message").field("fieldsStatus").exists()
        }),
        rendered: entries.iter().any(|entry| {
            entry.field("message").field("rendered").exists()
                || entry.field("message").field("renderStatus").exists()
        }),
        payload: entries
            .iter()
            .any(|entry| entry.field("message").field("payload").exists()),
        sample_value: entries
            .iter()
            .any(|entry| entry.field("sample").field("value").exists()),
        text: entries
            .iter()
            .any(|entry| entry.field("text").field("text").exists()),
        target: entries
            .iter()
            .any(|entry| entry.field("text").field("target").exists()),
        extension: entries
            .iter()
            .any(|entry| entry.field("extension").exists()),
    }
}
fn service<'a>(ctx: &Context<'a>) -> async_graphql::Result<&'a Service> {
    ctx.data::<Service>()
}
fn admission(ctx: &Context<'_>, bytes: usize) -> async_graphql::Result<()> {
    ctx.data::<Arc<RequestBudget>>()?
        .root(bytes)
        .map_err(|error| {
            let mut out = Error::new(error.message);
            out.extensions = error.extensions;
            out
        })
}
fn session_id(id: &str) -> Result<(), ServiceError> {
    if id.len() != 32
        || !id
            .bytes()
            .all(|c| c.is_ascii_hexdigit() && !c.is_ascii_uppercase())
    {
        return Err(ServiceError::InvalidInput(
            "invalid session capability".into(),
        ));
    }
    Ok(())
}
#[derive(Clone, Copy)]
enum ReadScope {
    Finite,
    Session,
    Follow,
}
fn query_parts(
    ctx: &Context<'_>,
    query: Option<QueryInput>,
    limits: Option<LimitsInput>,
    scope: ReadScope,
) -> async_graphql::Result<(dduroc_read::Query, dduroc_read::ReadLimits)> {
    let service = service(ctx)?;
    let query = query
        .unwrap_or_default()
        .native(
            &service.config().input_limits,
            matches!(scope, ReadScope::Follow),
        )
        .map_err(|error| error.graphql())?;
    let limits = limits
        .unwrap_or_default()
        .native(
            &service.config().read_limits,
            match scope {
                ReadScope::Follow => service.config().subscription_lifetime,
                ReadScope::Session => service.config().session_lifetime,
                ReadScope::Finite => service.config().operation_timeout,
            },
        )
        .map_err(|error| error.graphql())?;
    Ok((query, limits))
}

pub struct LogQuery;
#[Object]
impl LogQuery {
    async fn api_version(&self) -> &'static str {
        types::API_VERSION
    }
    /// Global retained counts are null until the count index is complete.
    async fn statistics(
        &self,
        ctx: &Context<'_>,
        limits: Option<LimitsInput>,
    ) -> async_graphql::Result<types::StatisticsView> {
        let service = service(ctx)?;
        let limits = limits
            .unwrap_or_default()
            .native(
                &service.config().read_limits,
                service.config().operation_timeout,
            )
            .map_err(|error| error.graphql())?;
        admission(ctx, 4096)?;
        service
            .statistics(limits, ctx.look_ahead().field("roots").exists())
            .await
            .map_err(|error| error.graphql())
    }
    async fn catalog(
        &self,
        ctx: &Context<'_>,
        query: Option<CatalogInput>,
        limits: Option<LimitsInput>,
    ) -> async_graphql::Result<CatalogView> {
        let service = service(ctx)?;
        let query = query
            .unwrap_or_default()
            .native(&service.config().input_limits)
            .map_err(|error| error.graphql())?;
        let limits = limits
            .unwrap_or_default()
            .native(
                &service.config().read_limits,
                service.config().operation_timeout,
            )
            .map_err(|error| error.graphql())?;
        admission(ctx, service.config().conversion_limits.max_output_bytes)?;
        service
            .catalog(query, limits)
            .await
            .map_err(|error| error.graphql())
    }
    async fn read(
        &self,
        ctx: &Context<'_>,
        query: Option<QueryInput>,
        limits: Option<LimitsInput>,
        page: Option<PageInput>,
        presentation: Option<PresentationInput>,
    ) -> async_graphql::Result<PageView> {
        let service = service(ctx)?;
        let (query, limits) = query_parts(ctx, query, limits, ReadScope::Finite)?;
        let page = page
            .unwrap_or_default()
            .native(
                service.config().max_page_records,
                service.config().max_page_bytes,
            )
            .map_err(|error| error.graphql())?;
        let mut presentation = presentation
            .unwrap_or_default()
            .native(service.language_preferences())
            .map_err(|error| error.graphql())?;
        presentation.projection = projection(ctx, false);
        admission(ctx, service.config().conversion_limits.max_output_bytes)?;
        service
            .query(query, limits, page, presentation)
            .await
            .map_err(|error| error.graphql())
    }
}
pub struct LogMutation;
#[Object]
impl LogMutation {
    /// Perform one bounded index-maintenance step; repeat until `more` is false.
    async fn backfill_statistics(
        &self,
        ctx: &Context<'_>,
        limits: Option<LimitsInput>,
    ) -> async_graphql::Result<types::BackfillStatisticsView> {
        let service = service(ctx)?;
        let limits = limits
            .unwrap_or_default()
            .native(
                &service.config().read_limits,
                service.config().operation_timeout,
            )
            .map_err(|error| error.graphql())?;
        admission(ctx, 1024)?;
        service
            .backfill_statistics(limits)
            .await
            .map_err(|error| error.graphql())
    }
    async fn open_read_session(
        &self,
        ctx: &Context<'_>,
        query: Option<QueryInput>,
        limits: Option<LimitsInput>,
        presentation: Option<PresentationInput>,
    ) -> async_graphql::Result<ReadSessionOpened> {
        let service = service(ctx)?;
        let (query, limits) = query_parts(ctx, query, limits, ReadScope::Session)?;
        let presentation = presentation
            .unwrap_or_default()
            .native(service.language_preferences())
            .map_err(|error| error.graphql())?;
        admission(ctx, 1024)?;
        let opened = service
            .open_session(query, limits, presentation)
            .await
            .map_err(|error| error.graphql())?;
        track(ctx, service, &opened.id)?;
        Ok(ReadSessionOpened {
            id: opened.id,
            revision: UInt64(opened.revision),
        })
    }
    async fn open_counted_read_session(
        &self,
        ctx: &Context<'_>,
        query: Option<QueryInput>,
        limits: Option<LimitsInput>,
        presentation: Option<PresentationInput>,
        page: PageInput,
    ) -> async_graphql::Result<CountedReadSessionOpened> {
        let service = service(ctx)?;
        let (query, limits) = query_parts(ctx, query, limits, ReadScope::Session)?;
        let presentation = presentation
            .unwrap_or_default()
            .native(service.language_preferences())
            .map_err(|error| error.graphql())?;
        let page = page
            .native(
                service.config().max_page_records,
                service.config().max_page_bytes,
            )
            .map_err(|error| error.graphql())?;
        admission(ctx, 1024)?;
        let opened = service
            .open_counted_session(query, limits, presentation, page)
            .await
            .map_err(|error| error.graphql())?;
        track(ctx, service, &opened.id)?;
        let totals = opened.totals;
        Ok(CountedReadSessionOpened {
            id: opened.id,
            revision: UInt64(opened.revision),
            total_matches: UInt64(totals.total_matches),
            total_seeds: UInt64(totals.total_seeds),
            total_pages: UInt64(totals.total_pages),
        })
    }
    async fn read_session(
        &self,
        ctx: &Context<'_>,
        id: String,
        expected_revision: UInt64,
        page: Option<PageInput>,
    ) -> async_graphql::Result<ReadSessionPage> {
        let service = service(ctx)?;
        session_id(&id).map_err(|error| error.graphql())?;
        let page = page
            .unwrap_or_default()
            .native(
                service.config().max_page_records,
                service.config().max_page_bytes,
            )
            .map_err(|error| error.graphql())?;
        admission(ctx, service.config().conversion_limits.max_output_bytes)?;
        let page = service
            .next_page(id.clone(), expected_revision.0, page, projection(ctx, true))
            .await
            .map_err(|error| error.graphql())?;
        track(ctx, service, &id)?;
        Ok(ReadSessionPage {
            revision: UInt64(page.revision),
            page: page.page,
        })
    }
    async fn seek_read_session(
        &self,
        ctx: &Context<'_>,
        id: String,
        expected_revision: UInt64,
        target_page: UInt64,
        page: Option<PageInput>,
        presentation: Option<PresentationInput>,
    ) -> async_graphql::Result<ReadSessionSeekPage> {
        let service = service(ctx)?;
        session_id(&id).map_err(|error| error.graphql())?;
        let page = page
            .unwrap_or_default()
            .native(
                service.config().max_page_records,
                service.config().max_page_bytes,
            )
            .map_err(|error| error.graphql())?;
        let mut presentation = presentation
            .unwrap_or_default()
            .native(service.language_preferences())
            .map_err(|error| error.graphql())?;
        presentation.projection = projection(ctx, true);
        admission(ctx, service.config().conversion_limits.max_output_bytes)?;
        let result = service
            .seek_page(
                id.clone(),
                expected_revision.0,
                target_page.0,
                presentation,
                page,
            )
            .await
            .map_err(|error| error.graphql())?;
        track(ctx, service, &id)?;
        Ok(ReadSessionSeekPage {
            revision: UInt64(result.revision),
            reached_page: UInt64(result.result.reached_page),
            target_reached: result.result.target_reached,
            exhausted: result.result.exhausted,
            page: result.result.page,
        })
    }
    async fn close_read_session(
        &self,
        ctx: &Context<'_>,
        id: String,
        expected_revision: UInt64,
    ) -> async_graphql::Result<bool> {
        session_id(&id).map_err(|error| error.graphql())?;
        admission(ctx, 1024)?;
        service(ctx)?
            .close(id, expected_revision.0)
            .await
            .map_err(|error| error.graphql())?;
        Ok(true)
    }
    async fn cancel_read_session(
        &self,
        ctx: &Context<'_>,
        id: String,
    ) -> async_graphql::Result<bool> {
        session_id(&id).map_err(|error| error.graphql())?;
        admission(ctx, 1024)?;
        service(ctx)?
            .cancel(id)
            .await
            .map_err(|error| error.graphql())?;
        Ok(true)
    }
}
pub struct LogSubscription;
#[Subscription]
impl LogSubscription {
    async fn follow(
        &self,
        ctx: &Context<'_>,
        query: Option<QueryInput>,
        limits: Option<LimitsInput>,
        presentation: Option<PresentationInput>,
    ) -> async_graphql::Result<impl Stream<Item = async_graphql::Result<FollowBatchView>>> {
        let service = service(ctx)?;
        let (query, limits) = query_parts(ctx, query, limits, ReadScope::Follow)?;
        let mut presentation = presentation
            .unwrap_or_default()
            .native(service.language_preferences())
            .map_err(|error| error.graphql())?;
        presentation.projection = projection(ctx, false);
        admission(ctx, service.config().conversion_limits.max_output_bytes)?;
        let stream = service
            .subscribe(query, limits, presentation)
            .map_err(|error| error.graphql())?;
        Ok(stream.map(|item| item.map_err(|error| error.graphql())))
    }
}
pub type LogSchema = Schema<LogQuery, LogMutation, LogSubscription>;
pub fn build_schema(service: Service, limits: SchemaLimits) -> Result<LogSchema, ServiceError> {
    if [
        limits.max_document_bytes,
        limits.max_variable_bytes,
        limits.max_variable_nodes,
        limits.max_selections,
        limits.max_depth,
        limits.max_complexity,
        limits.max_root_calls,
        limits.max_conversion_bytes,
        limits.max_response_bytes,
        limits.max_resolved_fields,
        limits.max_errors,
        limits.max_error_bytes,
    ]
    .contains(&0)
    {
        return Err(ServiceError::InvalidInput(
            "schema limits must be positive".into(),
        ));
    }
    Ok(Schema::build(LogQuery, LogMutation, LogSubscription)
        .data(service)
        .limit_depth(limits.max_depth)
        .limit_recursive_depth(limits.max_depth.saturating_mul(2))
        .limit_complexity(limits.max_complexity)
        .limit_directives(16)
        .extension(BudgetFactory(limits))
        .finish())
}
