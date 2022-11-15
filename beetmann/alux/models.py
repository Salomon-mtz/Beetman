from django.db import models
from flask_login import UserMixin
from werkzeug.security import check_password_hash, generate_password_hash


class User(UserMixin):

    def __init__(self, id, name, email, password, role,is_admin=False,carga_temporal=False):
        self.id = id
        self.name = name
        self.email = email
        if carga_temporal==True:
            self.password = password
        else:
            self.password = generate_password_hash(password)
        self.is_admin = is_admin
        self.role=role

    def set_password(self, password):
        self.password = generate_password_hash(password)

    def get_password(self):
        return str(self.password)

    def check_password(self, password):
        return check_password_hash(self.password, password)

    def __repr__(self):
        return '<User {}>'.format(self.email)
    
    def is_authenticated(self):
	    return True
    
    def is_active(self):
        return True

    def is_anonymous(self):
        return False

    def get_id(self):
        return str(self.id)

    def is_admin(self):
        return self.admin

    def get_role(self):
        return self.role

    def get_name(self):
        return self.name