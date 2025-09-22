# Current limitations of the Cypher -> AQL translation

 - No statements for schema changes
 - No modification statements (CREATE, DELETE, etc.)
 - For now, only one MATCH clause followed by a RETURN clause
 - Only simple edge label expr (not A|B or A|~B or things like this
 - Only local WHERE in nodes (no reference of outside variables)
 - Only local WHERE in edges (no reference of outside variables)
 - No OPTIONAL MATCH
 - Only JSON types (not time type, for example)
 - No UNION
 - No WITH

Not yet there, but still planned for the near future:

 - No SKIP
 - No LIMIT
 - No ORDER-BY
 - COUNT(*)
