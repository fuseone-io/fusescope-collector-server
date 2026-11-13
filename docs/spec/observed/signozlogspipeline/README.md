# Moved

The signozlogspipeline observation moved to
[`components/pkg/logspipeline/corpus/observed`](../../../../components/pkg/logspipeline/corpus/observed)
in ADR 0012 step 4. The `logspipeline` module publishes it as the canonical log pipelines corpus,
through its [`corpus`](../../../../components/pkg/logspipeline/corpus) package. The files are
unchanged: 223 cases and 88 startup outcomes of `fusescope-otel-collector` 0.145.0.
