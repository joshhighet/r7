// run the below script, upload the CSV as a reference list, name it public_suffix_list
// https://gist.github.com/joshhighet/0f68ffe324a895b1df189881d35fa196
MATCH (d:Rapid7EASMDomain) WITH d.name AS domain_name
WITH domain_name, split(domain_name, '.') AS parts
WHERE size(parts) >= 2  // ensure atleast domain.tld
// try match compound suffixes first (co.nz, com.au, etc)
WITH domain_name, parts,
     CASE 
       WHEN size(parts) >= 3 
       THEN parts[size(parts) - 2] + '.' + parts[size(parts) - 1]
       ELSE parts[size(parts) - 1]
     END AS potential_compound_suffix,
     parts[size(parts) - 1] AS simple_suffix
// check compound match
OPTIONAL MATCH (compound_psl:public_suffix_list) WHERE compound_psl.suffix = potential_compound_suffix
// check simple suffix match as fallback  
OPTIONAL MATCH (simple_psl:public_suffix_list) WHERE simple_psl.suffix = simple_suffix
// calc the apex domain by selecting the longest matching suffix and prepending the parent label
WITH domain_name, parts, potential_compound_suffix, simple_suffix, compound_psl, simple_psl,
     CASE
       WHEN compound_psl IS NOT NULL AND size(parts) >= 3
       THEN parts[size(parts) - 3] + '.' + potential_compound_suffix
       WHEN simple_psl IS NOT NULL AND size(parts) >= 2  
       THEN parts[size(parts) - 2] + '.' + simple_suffix
       ELSE domain_name  // hopefully only done when actually looking at an apex domain
     END AS apex_domain
// return domains by subdomain count
RETURN apex_domain, count(domain_name) AS subdomain_count
ORDER BY subdomain_count DESC