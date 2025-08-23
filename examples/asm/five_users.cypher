// 5 Users
// List 5 users in the environment
// Columns: [{"alias": "u", "property_name": "name"}]

MATCH (u:User) 
RETURN u 
LIMIT 5