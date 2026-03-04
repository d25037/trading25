/**
 * Local error hierarchy for phase4 package split.
 *
 * Keeping this package self-contained avoids pulling shared/src during
 * per-package builds.
 */
export abstract class Trading25Error extends Error {
  abstract readonly code: string;
  abstract readonly httpStatus: number;

  constructor(
    message: string,
    public override readonly cause?: Error
  ) {
    super(message);
    this.name = this.constructor.name;
  }
}

export class BadRequestError extends Trading25Error {
  readonly code: string = 'BAD_REQUEST';
  readonly httpStatus = 400 as const;
}

export class NotFoundError extends Trading25Error {
  readonly code: string = 'NOT_FOUND';
  readonly httpStatus = 404 as const;
}

export class ConflictError extends Trading25Error {
  readonly code: string = 'CONFLICT';
  readonly httpStatus = 409 as const;
}

export class InternalError extends Trading25Error {
  readonly code: string = 'INTERNAL_ERROR';
  readonly httpStatus = 500 as const;
}

export function isTrading25Error(error: unknown): error is Trading25Error {
  return error instanceof Trading25Error;
}

export function getErrorMessage(error: unknown): string {
  if (error instanceof Error) {
    return error.message;
  }
  return String(error);
}
