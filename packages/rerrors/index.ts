export type ValueResult<T> =
  | {
      success: true;
      value: T;
    }
  | {
      success: false;
      errors: string[];
    };

export type AnyValueResult = ValueResult<any>;

export type Result =
  | {
      success: true;
    }
  | {
      success: false;
      errors: string[];
    };

export const ErrorsToResponse = (errors: string[]): Response => {
  return new Response(errors.join(', '), { status: 400 });
};

export const SafeJSONParse = (json: string): AnyValueResult => {
  try {
    return {
      success: true,
      value: JSON.parse(json),
    };
  } catch (e) {
    return {
      success: false,
      errors: ['Unable to parse JSON'],
    };
  }
};
