db = db.getSiblingDB('admin');

if (!db.getUser("mongo")) {
    db.createUser({
        user: "mongo",
        pwd: "mongo",
        roles: [
            { role: "userAdminAnyDatabase", db: "admin" },
            { role: "readWriteAnyDatabase", db: "admin" }
        ]
    });
}

db = db.getSiblingDB('glamira');

if (!db.getUser("mongo")) {
    db.createUser({
        user: "mongo",
        pwd: "mongo",
        roles: [
            { role: "readWrite", db: "glamira" }
        ]
    });
} else {
    db.grantRolesToUser("mongo", [{ role: "readWrite", db: "glamira" }]);
}