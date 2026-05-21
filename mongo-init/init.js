// init.js
db = db.getSiblingDB('default_db');

db.createCollection('users');
db.users.insertOne({
  username: 'admin',
  role: 'superadmin',
  createdAt: new Date()
});

db.createCollection('settings');
db.settings.insertOne({
  key: 'app_name',
  value: 'MyApp'
});
