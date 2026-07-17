import type { paths } from '../clients/backtest/generated/bt-api-types';

type ApiPath = keyof paths;
type ApiMethod<Path extends ApiPath> = Exclude<keyof paths[Path], 'parameters'>;

/** The generated OpenAPI operation for a path and HTTP method. */
export type ApiOperation<Path extends ApiPath, Method extends ApiMethod<Path>> = NonNullable<paths[Path][Method]>;

type ApiParameter<Path extends ApiPath, Method extends ApiMethod<Path>, Location extends PropertyKey> =
  ApiOperation<Path, Method> extends { parameters: infer Parameters }
    ? Location extends keyof Parameters
      ? Exclude<Parameters[Location], undefined>
      : never
    : never;

/** Generated path parameters, or never when the operation has none. */
export type ApiPathParams<Path extends ApiPath, Method extends ApiMethod<Path>> = ApiParameter<Path, Method, 'path'>;

/** Generated query parameters, or never when the operation has none. */
export type ApiQuery<Path extends ApiPath, Method extends ApiMethod<Path>> = ApiParameter<Path, Method, 'query'>;

type ApiRequestBody<Path extends ApiPath, Method extends ApiMethod<Path>> =
  ApiOperation<Path, Method> extends { requestBody?: infer RequestBody } ? NonNullable<RequestBody> : never;

/** Generated application/json request body, or never when the operation has none. */
export type ApiJsonBody<Path extends ApiPath, Method extends ApiMethod<Path>> = [ApiRequestBody<Path, Method>] extends [
  never,
]
  ? never
  : ApiRequestBody<Path, Method> extends {
        content: { 'application/json': infer Body };
      }
    ? Body
    : never;

type ApiResponses<Path extends ApiPath, Method extends ApiMethod<Path>> =
  ApiOperation<Path, Method> extends { responses: infer Responses } ? Responses : never;

/** Generated application/json response body for a documented status. */
export type ApiJsonResponse<
  Path extends ApiPath,
  Method extends ApiMethod<Path>,
  Status extends keyof ApiResponses<Path, Method>,
> = ApiResponses<Path, Method>[Status] extends {
  content: { 'application/json': infer Response };
}
  ? Response
  : never;
