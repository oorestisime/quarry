export type { IntrospectionObjectDescriptor } from "./types";
export { describeDDLObjects, parseSchemaDDL } from "./ddl-parser";
import { generateSchemaModuleFromParsed } from "./codegen";
import { parseSchemaDDL } from "./ddl-parser";

export function generateSchemaModuleFromDDL(ddl: string): string {
  return generateSchemaModuleFromParsed(parseSchemaDDL(ddl));
}
