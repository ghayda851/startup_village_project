import classNames, { type Argument } from 'classnames';

export function cn(...inputs: Argument[]) {
  return classNames(inputs);
}
