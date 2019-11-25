from marshmallow import Schema, fields


class SchemaResponseError(Schema):
    data = None
    error = fields.String()


class SchemaSeries(Schema):
    name = fields.String()
    datapoint_count = fields.Integer()
    analysed = fields.Boolean()
    new_forecast_at = fields.String()
    model = fields.Integer()
    model_parameters = fields.List(fields.String())
    ignore = fields.String()
    error = fields.String()


class SchemaResponseSeries(Schema):
    data = fields.List(fields.Nested(SchemaSeries))


class SchemaResponseSeriesDetails(Schema):
    name = fields.String()
    datapoint_count = fields.Integer()
    analysed = fields.Boolean()
    new_forecast_at = fields.String()
    model = fields.Integer()
    model_parameters = fields.List(fields.String())
    ignore = fields.String()
    error = fields.String()
    points = fields.List(fields.List(fields.Integer()))
    forecast_points = fields.List(fields.List(fields.Integer()))


class SchemaRequestCreateSeries(Schema):
    name = fields.String()
    model = fields.Integer()
    model_parameters = fields.List(fields.String())


class SchemaRequestCreateEnodoEventOutput(Schema):
    output_type = fields.String()
    data = fields.Dict()


class SchemaRequestCreateEnodoModel(Schema):
    model_name = fields.String()
    model_arguments = fields.List(fields.Dict())
    supports_forecasting = fields.Boolean()
    supports_anomaly_detection = fields.Boolean()


class _SchemaModelNameListItem(Schema):
    model_id = fields.Integer()
    model_name = fields.String()


class _SchemaModelParameterListItem(Schema):
    parameter_name = fields.String()
    parameter_value = fields.String()


class _SchemaResponseModelsNested(Schema):
    models = fields.List(fields.Nested(_SchemaModelNameListItem))
    parameters = fields.List(fields.Nested(_SchemaModelParameterListItem))


class SchemaResponseModels(Schema):
    data = fields.Nested(_SchemaResponseModelsNested)
