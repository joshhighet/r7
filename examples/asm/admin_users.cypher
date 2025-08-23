// Administrative Users
// List 10 users with administrative privileges
// Columns: [{"alias": "u", "property_name": "name"}]

MATCH (u:User) 
WHERE u.is_administrator 
RETURN u 
LIMIT 10