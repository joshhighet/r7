// Multi-Platform Users
// Find users present in multiple identity systems for access analysis

MATCH (u:User) 
WHERE size(u.sources) > 1 
RETURN u.name, 
       u.sources, 
       size(u.sources) as source_count 
ORDER BY source_count DESC