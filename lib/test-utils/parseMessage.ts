export const parseMessage = ({ value }: { value: any }) =>
  JSON.parse(value.Body);
