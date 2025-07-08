"""Authentication routes."""
from flask import Blueprint, render_template, redirect, url_for, flash, request, session
from flask_login import login_user, logout_user, login_required, current_user
from flask_wtf import FlaskForm
from wtforms import StringField, PasswordField, SubmitField
from wtforms.validators import DataRequired, Length
from app.auth.models import User
import secrets

auth = Blueprint('auth', __name__, url_prefix='/auth')


class LoginForm(FlaskForm):
    """Admin login form."""
    admin_key = PasswordField('Admin Key', validators=[
        DataRequired(),
        Length(min=8, message="Admin key must be at least 8 characters")
    ])
    submit = SubmitField('Login')


@auth.route('/login', methods=['GET', 'POST'])
def login():
    """Admin login page."""
    if current_user.is_authenticated:
        return redirect(url_for('admin.dashboard'))
    
    form = LoginForm()
    if form.validate_on_submit():
        if User.check_admin_key(form.admin_key.data):
            user = User('admin')
            login_user(user, remember=False)
            
            # Generate CSRF token for session
            session['csrf_token'] = secrets.token_hex(16)
            
            flash('Logged in successfully.', 'success')
            next_page = request.args.get('next')
            if next_page:
                return redirect(next_page)
            return redirect(url_for('admin.dashboard'))
        else:
            flash('Invalid admin key.', 'danger')
    
    return render_template('auth/login.html', form=form)


@auth.route('/logout')
@login_required
def logout():
    """Logout admin user."""
    logout_user()
    session.clear()
    flash('You have been logged out.', 'info')
    return redirect(url_for('web.index'))