create or replace function RAVEN.FN_ARRAY_STRIP_NULLS(string_in VARCHAR(16777216), delimiter_in VARCHAR(50))
returns string
language python
runtime_version = '3.8'
handler = 'run'
as
$$
def run(string_in,delimiter_in):
  return delimiter_in.join([x for x in string_in.split(delimiter_in) if x])
$$
;
