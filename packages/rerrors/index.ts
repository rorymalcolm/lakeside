export type ValueResult<T> =
  | {
      success: true;
      value: T;
    }
  | {
      success: false;
      errors: string[];
    };

export type Result =
  | {
      success: true;
    }
  | {
      success: false;
      errors: string[];
    };

export const ErrorsToResponse = (errors: string[]): Response => {
  return new Response(errors.join(", "), { status: 400 });
};
