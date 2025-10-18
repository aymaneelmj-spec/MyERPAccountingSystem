#!/usr/bin/env python3
"""
Happy Deal Transit ERP - FIXED Backend with Enhanced Error Handling
Version: 3.3-STABLE
"""
from flask import Flask, request, jsonify
from flask_cors import CORS
from flask_sqlalchemy import SQLAlchemy
from flask_jwt_extended import JWTManager, create_access_token, jwt_required, get_jwt_identity
from werkzeug.security import generate_password_hash, check_password_hash
from werkzeug.utils import secure_filename
from datetime import datetime, timedelta, date

import os
import csv
import io
import json
import traceback

try:
    import pandas as pd
    import openpyxl
    PANDAS_AVAILABLE = True
except ImportError:
    PANDAS_AVAILABLE = False
    print("Warning: pandas/openpyxl not available. File import limited to CSV only.")

from config import Config

# Import AI service with graceful fallback
try:
    from ai_service import categorize_transaction, detect_anomalies, forecast_cash_flow, get_spending_insights
    AI_AVAILABLE = True
except ImportError as e:
    print(f"Warning: AI service not available ({e}). Using fallback functions.")
    AI_AVAILABLE = False
    
    def categorize_transaction(description):
        return "Other"
    
    def detect_anomalies(transactions):
        return []
    
    def forecast_cash_flow(dates, amounts, steps=30):
        if not amounts:
            return [0] * steps
        avg = sum(amounts) / len(amounts) if amounts else 0
        return [avg] * steps
    
    def get_spending_insights(transactions, period_days=30):
        return {
            "total_spent": 0,
            "average_daily": 0,
            "top_categories": [],
            "trend": "stable"
        }

# Initialize Flask app
app = Flask(__name__)
app.config.from_object(Config)

# Ensure instance directory exists
os.makedirs(os.path.join(app.instance_path, 'uploads'), exist_ok=True)

# Initialize extensions
db = SQLAlchemy(app)
jwt = JWTManager(app)

# Enhanced CORS Configuration
CORS(app, 
     origins=['http://localhost:3000', 'http://127.0.0.1:3000', 'http://192.168.1.101:3000', 
              'https://*.vercel.app', 'https://myerp-frontend.vercel.app'], 'https://my-erp-frontend-delta.vercel.app',
     supports_credentials=True,
     allow_headers=['Content-Type', 'Authorization', 'Access-Control-Allow-Credentials'],
     methods=['GET', 'POST', 'PUT', 'DELETE', 'OPTIONS'],
     expose_headers=['Content-Type', 'Authorization']
)

# ============ DATABASE MODELS ============

class Company(db.Model):
    __tablename__ = 'companies'
    
    id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.String(200), nullable=False)
    address = db.Column(db.Text)
    phone = db.Column(db.String(50))
    email = db.Column(db.String(120))
    tax_id = db.Column(db.String(50))
    base_currency = db.Column(db.String(3), default='MAD')
    status = db.Column(db.String(20), default='active')
    created_at = db.Column(db.DateTime, default=datetime.utcnow)
    
    # Relationships
    users = db.relationship('User', backref='company', lazy=True, cascade='all, delete-orphan')
    transactions = db.relationship('Transaction', backref='company', lazy=True, cascade='all, delete-orphan')
    invoices = db.relationship('Invoice', backref='company', lazy=True, cascade='all, delete-orphan')
    inventory_items = db.relationship('InventoryItem', backref='company', lazy=True, cascade='all, delete-orphan')
    entries = db.relationship('DataEntry', backref='company', lazy=True, cascade='all, delete-orphan')

class User(db.Model):
    __tablename__ = 'users'
    
    id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.String(100), nullable=False)
    email = db.Column(db.String(120), unique=True, nullable=False, index=True)
    password_hash = db.Column(db.String(255), nullable=False)
    role = db.Column(db.String(20), default='user', index=True)
    company_id = db.Column(db.Integer, db.ForeignKey('companies.id'), nullable=False, index=True)
    status = db.Column(db.String(20), default='active', index=True)
    created_at = db.Column(db.DateTime, default=datetime.utcnow)
    last_login = db.Column(db.DateTime)
    
    def set_password(self, password):
        self.password_hash = generate_password_hash(password)
    
    def check_password(self, password):
        return check_password_hash(self.password_hash, password)

class Transaction(db.Model):
    __tablename__ = 'transactions'
    
    id = db.Column(db.Integer, primary_key=True)
    company_id = db.Column(db.Integer, db.ForeignKey('companies.id'), nullable=False, index=True)
    user_id = db.Column(db.Integer, db.ForeignKey('users.id'), nullable=True, index=True)
    date = db.Column(db.Date, nullable=False, index=True)
    description = db.Column(db.String(500), nullable=False)
    
    # Multi-currency support
    amount = db.Column(db.Float, nullable=False)
    currency = db.Column(db.String(3), default='MAD')
    original_currency = db.Column(db.String(3), default='MAD')
    amount_mad = db.Column(db.Float, nullable=False, index=True)
    exchange_rate = db.Column(db.Float, default=1.0)
    exchange_rate_date = db.Column(db.DateTime, default=datetime.utcnow)
    
    type = db.Column(db.String(20), nullable=False, index=True)  # income/expense
    category = db.Column(db.String(100), index=True)
    source = db.Column(db.String(50), default='manual')
    created_at = db.Column(db.DateTime, default=datetime.utcnow)

class Invoice(db.Model):
    __tablename__ = 'invoices'
    
    id = db.Column(db.Integer, primary_key=True)
    company_id = db.Column(db.Integer, db.ForeignKey('companies.id'), nullable=False, index=True)
    user_id = db.Column(db.Integer, db.ForeignKey('users.id'), nullable=True)
    invoice_number = db.Column(db.String(50), nullable=False, unique=True, index=True)
    client_name = db.Column(db.String(200), nullable=False)
    client_email = db.Column(db.String(120))
    
    amount = db.Column(db.Float, nullable=False)
    currency = db.Column(db.String(3), default='MAD')
    tax_amount = db.Column(db.Float, default=0)
    total_amount = db.Column(db.Float, nullable=False)
    
    date_created = db.Column(db.Date, nullable=False, index=True)
    date_due = db.Column(db.Date, index=True)
    status = db.Column(db.String(20), default='pending', index=True)
    description = db.Column(db.Text)
    created_at = db.Column(db.DateTime, default=datetime.utcnow)

class InventoryItem(db.Model):
    __tablename__ = 'inventory_items'
    
    id = db.Column(db.Integer, primary_key=True)
    company_id = db.Column(db.Integer, db.ForeignKey('companies.id'), nullable=False, index=True)
    user_id = db.Column(db.Integer, db.ForeignKey('users.id'), nullable=True)
    name = db.Column(db.String(200), nullable=False)
    category = db.Column(db.String(100), index=True)
    quantity = db.Column(db.Integer, default=0)
    
    unit_price = db.Column(db.Float, nullable=False)
    currency = db.Column(db.String(3), default='MAD')
    unit_price_mad = db.Column(db.Float, nullable=False)
    
    created_at = db.Column(db.DateTime, default=datetime.utcnow)

class DataEntry(db.Model):
    __tablename__ = 'data_entries'
    
    id = db.Column(db.Integer, primary_key=True)
    company_id = db.Column(db.Integer, db.ForeignKey('companies.id'), nullable=False, index=True)
    user_id = db.Column(db.Integer, db.ForeignKey('users.id'), nullable=False, index=True)
    entry_type = db.Column(db.String(50), nullable=False, index=True)
    
    data = db.Column(db.Text, nullable=False)
    
    title = db.Column(db.String(200))
    description = db.Column(db.Text)
    status = db.Column(db.String(20), default='active', index=True)
    created_by = db.Column(db.Integer, db.ForeignKey('users.id'), nullable=False)
    created_at = db.Column(db.DateTime, default=datetime.utcnow)
    updated_at = db.Column(db.DateTime, default=datetime.utcnow)

class ExchangeRate(db.Model):
    __tablename__ = 'exchange_rates'
    
    id = db.Column(db.Integer, primary_key=True)
    base_currency = db.Column(db.String(3), default='MAD')
    target_currency = db.Column(db.String(3), nullable=False, index=True)
    rate = db.Column(db.Float, nullable=False)
    date = db.Column(db.Date, nullable=False, index=True)
    source = db.Column(db.String(50))
    created_at = db.Column(db.DateTime, default=datetime.utcnow)

# ============ ROLE-BASED ACCESS CONTROL ============

def get_user_from_token():
    """Get user from JWT token with error handling"""
    try:
        user_id = get_jwt_identity()
        if not user_id:
            return None
        user = User.query.filter_by(id=user_id, status='active').first()
        if user:
            user.last_login = datetime.utcnow()
            db.session.commit()
        return user
    except Exception as e:
        print(f"Error getting user from token: {e}")
        return None

def is_admin(user):
    """Check if user has admin privileges"""
    return user and user.role in ['admin', 'administrator']

def can_access_company_data(user, company_id):
    """Check if user can access company data"""
    if not user:
        return False
    
    # Convert to same type for comparison
    try:
        user_company_id = int(user.company_id)
        target_company_id = int(company_id)
    except (ValueError, TypeError):
        return False
    
    # Admin can access all companies
    if is_admin(user):
        return True
    
    # Regular users can only access their own company
    return user_company_id == target_company_id

# ============ EXCHANGE RATE SERVICE ============

class ExchangeRateService:
    def __init__(self):
        self.cache = {}
        self.last_update = None
        self.update_interval = 300  # 5 minutes
        
    def get_live_rates(self, base='MAD'):
        """Get exchange rates with caching"""
        current_time = datetime.now()
        cache_key = f"{base}_{current_time.strftime('%Y%m%d_%H%M')}"
        
        # Check cache
        if (cache_key in self.cache and 
            self.last_update and 
            (current_time - self.last_update).seconds < self.update_interval):
            return self.cache[cache_key]
        
        # Get rates
        rates = self._get_realistic_rates(base)
        
        if rates:
            self.cache[cache_key] = rates
            self.last_update = current_time
            self._store_rates_in_db(rates, base)
        
        return rates
    
    def _get_realistic_rates(self, base='MAD'):
        """Get realistic exchange rates (would connect to API in production)"""
        # Realistic MAD exchange rates (as of 2024)
        base_rates_to_mad = {
            'USD': 10.12,
            'EUR': 11.05,
            'GBP': 12.78,
            'MAD': 1.0
        }
        
        if base == 'MAD':
            rates = {}
            for currency, rate_to_mad in base_rates_to_mad.items():
                if currency == 'MAD':
                    rates[currency] = 1.0
                else:
                    rates[currency] = 1.0 / rate_to_mad
            return rates
        else:
            return base_rates_to_mad
    
    def _store_rates_in_db(self, rates, base='MAD'):
        """Store exchange rates in database"""
        try:
            today = date.today()
            
            for currency, rate in rates.items():
                if currency != base:
                    existing = ExchangeRate.query.filter_by(
                        base_currency=base,
                        target_currency=currency,
                        date=today
                    ).first()
                    
                    if existing:
                        existing.rate = rate
                    else:
                        new_rate = ExchangeRate(
                            base_currency=base,
                            target_currency=currency,
                            rate=rate,
                            date=today,
                            source='realistic'
                        )
                        db.session.add(new_rate)
            
            db.session.commit()
            
        except Exception as e:
            print(f"Error storing rates in DB: {e}")
            db.session.rollback()
    
    def convert_currency(self, amount, from_currency, to_currency):
        """Convert amount between currencies"""
        if from_currency == to_currency:
            return amount
        
        try:
            rates = self.get_live_rates('MAD')
            
            if from_currency == 'MAD':
                return amount * rates.get(to_currency, 1)
            elif to_currency == 'MAD':
                return amount / rates.get(from_currency, 1)
            else:
                # Convert through MAD
                amount_in_mad = amount / rates.get(from_currency, 1)
                return amount_in_mad * rates.get(to_currency, 1)
        except Exception as e:
            print(f"Currency conversion error: {e}")
            return amount

exchange_service = ExchangeRateService()

# ============ ERROR HANDLERS ============

@app.errorhandler(404)
def not_found(error):
    return jsonify({'error': 'Endpoint not found'}), 404

@app.errorhandler(500)
def internal_error(error):
    db.session.rollback()
    return jsonify({'error': 'Internal server error'}), 500

@app.errorhandler(Exception)
def handle_exception(e):
    db.session.rollback()
    print(f"Unhandled exception: {e}")
    traceback.print_exc()
    return jsonify({'error': 'An unexpected error occurred'}), 500

@app.before_request
def handle_preflight():
    if request.method == "OPTIONS":
        response = jsonify({})
        response.headers.add("Access-Control-Allow-Origin", "*")
        response.headers.add('Access-Control-Allow-Headers', "*")
        response.headers.add('Access-Control-Allow-Methods', "*")
        return response

# ============ API ROUTES ============

@app.route('/api/test', methods=['GET'])
def test():
    return jsonify({
        'message': 'Happy Deal Transit ERP API is working!',
        'status': 'success',
        'version': '3.3-STABLE',
        'features': ['role-based-access', 'multi-currency', 'enhanced-error-handling'],
        'timestamp': datetime.now().isoformat(),
        'ai_available': AI_AVAILABLE,
        'pandas_available': PANDAS_AVAILABLE
    })

@app.route('/api/health', methods=['GET'])
def health():
    """Health check endpoint"""
    try:
        # Test database connection
        db.session.execute('SELECT 1')
        db_status = 'healthy'
    except Exception as e:
        db_status = f'error: {str(e)}'
    
    return jsonify({
        'status': 'healthy' if db_status == 'healthy' else 'degraded',
        'database': db_status,
        'timestamp': datetime.now().isoformat()
    })

# ============ AUTHENTICATION ROUTES ============

@app.route('/api/login', methods=['POST'])
def login():
    try:
        data = request.get_json()
        if not data:
            return jsonify({'error': 'No data provided'}), 400
            
        email = data.get('email', '').strip()
        password = data.get('password', '')
        
        if not email or not password:
            return jsonify({'error': 'Email and password required'}), 400
        
        user = User.query.filter_by(email=email, status='active').first()
        
        if user and user.check_password(password):
            # Update last login
            user.last_login = datetime.utcnow()
            db.session.commit()
            
            access_token = create_access_token(identity=user.id)
            
            return jsonify({
                'access_token': access_token,
                'user': {
                    'id': user.id,
                    'name': user.name,
                    'email': user.email,
                    'role': user.role,
                    'company_id': user.company_id
                }
            })
        
        return jsonify({'error': 'Invalid credentials'}), 401
        
    except Exception as e:
        print(f"Login error: {e}")
        traceback.print_exc()
        return jsonify({'error': 'Login failed'}), 500

@app.route('/api/user/profile', methods=['GET'])
@jwt_required()
def get_user_profile():
    try:
        user = get_user_from_token()
        
        if not user:
            return jsonify({'error': 'User not found'}), 404
        
        return jsonify({
            'id': user.id,
            'name': user.name,
            'email': user.email,
            'role': user.role,
            'company_id': user.company_id,
            'last_login': user.last_login.isoformat() if user.last_login else None
        })
    except Exception as e:
        print(f"Profile error: {e}")
        return jsonify({'error': 'Failed to get profile'}), 500

# ============ EXCHANGE RATE ROUTES ============

@app.route('/api/exchange-rates', methods=['GET'])
def get_exchange_rates():
    try:
        base = request.args.get('base', 'MAD')
        rates = exchange_service.get_live_rates(base)
        
        return jsonify({
            'base_currency': base,
            'rates': rates,
            'last_update': exchange_service.last_update.isoformat() if exchange_service.last_update else None,
            'timestamp': datetime.now().isoformat()
        })
    except Exception as e:
        print(f"Exchange rates error: {e}")
        return jsonify({'error': 'Failed to get exchange rates'}), 500

# ============ DASHBOARD ROUTES ============

@app.route('/api/dashboard', methods=['GET'])
@jwt_required()
def get_dashboard_stats():
    try:
        user = get_user_from_token()
        if not user:
            return jsonify({'error': 'User not found'}), 404
            
        company_id = request.args.get('company_id', user.company_id)
        display_currency = request.args.get('currency', 'MAD')
        
        if not can_access_company_data(user, company_id):
            return jsonify({'error': 'Access denied'}), 403
        
        # Get transactions
        transactions = Transaction.query.filter_by(company_id=company_id).all()
        
        total_income = 0
        total_expenses = 0
        
        for transaction in transactions:
            try:
                converted_amount = exchange_service.convert_currency(
                    transaction.amount, 
                    transaction.currency or 'MAD', 
                    display_currency
                )
                
                if transaction.type == 'income':
                    total_income += converted_amount
                else:
                    total_expenses += converted_amount
            except Exception as e:
                print(f"Error processing transaction {transaction.id}: {e}")
        
        # Get pending invoices
        pending_invoices = Invoice.query.filter_by(
            company_id=company_id, 
            status='pending'
        ).count()
        
        # Get inventory value
        inventory_items = InventoryItem.query.filter_by(company_id=company_id).all()
        inventory_value = 0
        
        for item in inventory_items:
            try:
                item_value = item.quantity * item.unit_price
                converted_value = exchange_service.convert_currency(
                    item_value, 
                    item.currency or 'MAD', 
                    display_currency
                )
                inventory_value += converted_value
            except Exception as e:
                print(f"Error processing inventory item {item.id}: {e}")
        
        return jsonify({
            'total_income': round(total_income, 2),
            'total_expenses': round(total_expenses, 2),
            'net_profit': round(total_income - total_expenses, 2),
            'pending_invoices': pending_invoices,
            'inventory_value': round(inventory_value, 2),
            'display_currency': display_currency,
            'last_updated': datetime.now().isoformat(),
            'user_role': user.role
        })
    except Exception as e:
        print(f"Dashboard error: {e}")
        traceback.print_exc()
        return jsonify({'error': 'Failed to get dashboard stats'}), 500

@app.route('/api/dashboard/charts', methods=['GET'])
@jwt_required()
def get_chart_data():
    try:
        user = get_user_from_token()
        if not user:
            return jsonify({'error': 'User not found'}), 404
            
        company_id = request.args.get('company_id', user.company_id)
        period = request.args.get('period', '6months')
        display_currency = request.args.get('currency', 'MAD')
        
        if not can_access_company_data(user, company_id):
            return jsonify({'error': 'Access denied'}), 403
        
        # Calculate date range
        end_date = date.today()
        if period == 'weekly':
            start_date = end_date - timedelta(days=7)
            date_format = '%Y-%m-%d'
        elif period == 'monthly':
            start_date = end_date.replace(day=1)
            date_format = '%Y-%m-%d'
        elif period == 'yearly':
            start_date = end_date.replace(month=1, day=1)
            date_format = '%Y-%m'
        else:  # 6months
            start_date = end_date - timedelta(days=180)
            date_format = '%Y-%m'
        
        # Get transactions in range
        transactions = Transaction.query.filter(
            Transaction.company_id == company_id,
            Transaction.date >= start_date,
            Transaction.date <= end_date
        ).all()
        
        # Group by period
        period_data = {}
        category_data = {}
        
        for transaction in transactions:
            try:
                converted_amount = exchange_service.convert_currency(
                    transaction.amount, 
                    transaction.currency or 'MAD', 
                    display_currency
                )
                
                # Time period grouping
                period_key = transaction.date.strftime(date_format)
                
                if period_key not in period_data:
                    period_data[period_key] = {'period': period_key, 'income': 0, 'expenses': 0}
                
                if transaction.type == 'income':
                    period_data[period_key]['income'] += converted_amount
                else:
                    period_data[period_key]['expenses'] += converted_amount
                    
                    # Category data for expenses
                    category = transaction.category or 'Other'
                    if category not in category_data:
                        category_data[category] = 0
                    category_data[category] += converted_amount
            except Exception as e:
                print(f"Error processing transaction {transaction.id}: {e}")
        
        # Convert to arrays
        monthly_data = list(period_data.values())
        monthly_data.sort(key=lambda x: x['period'])
        
        category_data_array = [
            {'category': cat, 'amount': round(amount, 2)} 
            for cat, amount in category_data.items()
        ]
        category_data_array.sort(key=lambda x: x['amount'], reverse=True)
        
        return jsonify({
            'monthly_data': monthly_data,
            'category_data': category_data_array,
            'period': period,
            'display_currency': display_currency
        })
    except Exception as e:
        print(f"Chart data error: {e}")
        traceback.print_exc()
        return jsonify({'error': 'Failed to get chart data'}), 500

# ============ TRANSACTION ROUTES ============

@app.route('/api/transactions', methods=['GET'])
@jwt_required()
def get_transactions():
    try:
        user = get_user_from_token()
        if not user:
            return jsonify({'error': 'User not found'}), 404
            
        company_id = request.args.get('company_id', user.company_id)
        
        if not can_access_company_data(user, company_id):
            return jsonify({'error': 'Access denied'}), 403
        
        # Get query parameters for filtering
        transaction_type = request.args.get('type')
        category = request.args.get('category')
        start_date = request.args.get('start_date')
        end_date = request.args.get('end_date')
        
        query = Transaction.query.filter_by(company_id=company_id)
        
        # Apply filters
        if transaction_type:
            query = query.filter_by(type=transaction_type)
        if category:
            query = query.filter_by(category=category)
        if start_date:
            query = query.filter(Transaction.date >= datetime.strptime(start_date, '%Y-%m-%d').date())
        if end_date:
            query = query.filter(Transaction.date <= datetime.strptime(end_date, '%Y-%m-%d').date())
        
        transactions = query.order_by(Transaction.date.desc()).all()
        
        return jsonify([{
            'id': t.id,
            'company_id': t.company_id,
            'user_id': t.user_id,
            'date': t.date.isoformat(),
            'description': t.description,
            'amount': t.amount,
            'currency': t.currency or 'MAD',
            'original_currency': t.original_currency or t.currency or 'MAD',
            'amount_mad': t.amount_mad,
            'exchange_rate': t.exchange_rate,
            'type': t.type,
            'category': t.category,
            'source': t.source,
            'created_at': t.created_at.isoformat()
        } for t in transactions])
    except Exception as e:
        print(f"Get transactions error: {e}")
        traceback.print_exc()
        return jsonify({'error': 'Failed to get transactions'}), 500

@app.route('/api/transactions', methods=['POST'])
@jwt_required()
def create_transaction():
    try:
        user = get_user_from_token()
        if not user:
            return jsonify({'error': 'User not found'}), 404
            
        data = request.get_json()
        if not data:
            return jsonify({'error': 'No data provided'}), 400
        
        # Validate required fields
        required_fields = ['date', 'description', 'amount', 'type']
        for field in required_fields:
            if field not in data:
                return jsonify({'error': f'Missing required field: {field}'}), 400
        
        company_id = data.get('company_id', user.company_id)
        if not can_access_company_data(user, company_id):
            return jsonify({'error': 'Access denied'}), 403
        
        # Validate transaction type
        if data['type'] not in ['income', 'expense']:
            return jsonify({'error': 'Type must be income or expense'}), 400
        
        currency = data.get('currency', 'MAD')
        amount = float(data['amount'])
        
        if amount <= 0:
            return jsonify({'error': 'Amount must be positive'}), 400
        
        # Convert to MAD for storage
        if currency != 'MAD':
            amount_mad = exchange_service.convert_currency(amount, currency, 'MAD')
            rates = exchange_service.get_live_rates('MAD')
            exchange_rate = 1.0 / rates.get(currency, 1.0)
        else:
            amount_mad = amount
            exchange_rate = 1.0
        
        # Auto-categorize if no category provided
        category = data.get('category', '')
        if not category and AI_AVAILABLE:
            category = categorize_transaction(data['description'])
        
        transaction = Transaction(
            company_id=company_id,
            user_id=user.id,
            date=datetime.strptime(data['date'], '%Y-%m-%d').date(),
            description=data['description'][:500],
            amount=amount,
            currency=currency,
            original_currency=currency,
            amount_mad=amount_mad,
            exchange_rate=exchange_rate,
            type=data['type'],
            category=category,
            source=data.get('source', 'manual')
        )
        
        db.session.add(transaction)
        db.session.commit()
        
        return jsonify({
            'message': 'Transaction created successfully',
            'id': transaction.id,
            'amount_mad': round(amount_mad, 2),
            'exchange_rate': exchange_rate,
            'category': category
        }), 201
    except ValueError as e:
        return jsonify({'error': f'Invalid data format: {str(e)}'}), 400
    except Exception as e:
        print(f"Create transaction error: {e}")
        traceback.print_exc()
        db.session.rollback()
        return jsonify({'error': 'Failed to create transaction'}), 500

@app.route('/api/transactions/bulk-import', methods=['POST'])
@jwt_required()
def bulk_import_transactions():
    try:
        user = get_user_from_token()
        if not user:
            return jsonify({'error': 'User not found'}), 404
            
        data = request.get_json()
        if not data or not isinstance(data, list):
            return jsonify({'error': 'Invalid data format. Expected array'}), 400
        
        imported_count = 0
        errors = []
        
        for idx, row in enumerate(data):
            try:
                company_id = row.get('company_id', user.company_id)
                if not can_access_company_data(user, company_id):
                    errors.append(f'Row {idx + 1}: Access denied')
                    continue
                
                # Validate required fields
                if not all(k in row for k in ['date', 'description', 'amount', 'type']):
                    errors.append(f'Row {idx + 1}: Missing required fields')
                    continue
                
                currency = row.get('currency', 'MAD')
                amount = float(row['amount'])
                
                if amount <= 0:
                    errors.append(f'Row {idx + 1}: Amount must be positive')
                    continue
                
                # Convert to MAD
                if currency != 'MAD':
                    amount_mad = exchange_service.convert_currency(amount, currency, 'MAD')
                    rates = exchange_service.get_live_rates('MAD')
                    exchange_rate = 1.0 / rates.get(currency, 1.0)
                else:
                    amount_mad = amount
                    exchange_rate = 1.0
                
                # Auto-categorize
                category = row.get('category', '')
                if not category and AI_AVAILABLE:
                    category = categorize_transaction(row['description'])
                
                transaction = Transaction(
                    company_id=company_id,
                    user_id=user.id,
                    date=datetime.strptime(row['date'], '%Y-%m-%d').date(),
                    description=row['description'][:500],
                    amount=amount,
                    currency=currency,
                    original_currency=currency,
                    amount_mad=amount_mad,
                    exchange_rate=exchange_rate,
                    type=row['type'],
                    category=category,
                    source='bulk_import'
                )
                
                db.session.add(transaction)
                imported_count += 1
                
            except Exception as e:
                errors.append(f'Row {idx + 1}: {str(e)}')
        
        if imported_count > 0:
            db.session.commit()
        
        return jsonify({
            'message': 'Bulk import completed',
            'imported_count': imported_count,
            'total_rows': len(data),
            'errors': errors
        })
        
    except Exception as e:
        print(f"Bulk import error: {e}")
        traceback.print_exc()
        db.session.rollback()
        return jsonify({'error': 'Failed to bulk import transactions'}), 500

@app.route('/api/transactions/<int:transaction_id>', methods=['PUT'])
@jwt_required()
def update_transaction(transaction_id):
    try:
        user = get_user_from_token()
        if not user:
            return jsonify({'error': 'User not found'}), 404
            
        transaction = Transaction.query.get_or_404(transaction_id)
        
        if not can_access_company_data(user, transaction.company_id):
            return jsonify({'error': 'Access denied'}), 403
        
        data = request.get_json()
        if not data:
            return jsonify({'error': 'No data provided'}), 400
        
        # Update transaction fields
        if 'date' in data:
            transaction.date = datetime.strptime(data['date'], '%Y-%m-%d').date()
        if 'description' in data:
            transaction.description = data['description'][:500]
        if 'amount' in data:
            transaction.amount = float(data['amount'])
        if 'currency' in data:
            transaction.currency = data['currency']
        if 'type' in data:
            if data['type'] not in ['income', 'expense']:
                return jsonify({'error': 'Type must be income or expense'}), 400
            transaction.type = data['type']
        if 'category' in data:
            transaction.category = data['category']
        
        # Recalculate MAD amount
        if transaction.currency != 'MAD':
            transaction.amount_mad = exchange_service.convert_currency(
                transaction.amount, transaction.currency, 'MAD'
            )
            rates = exchange_service.get_live_rates('MAD')
            transaction.exchange_rate = 1.0 / rates.get(transaction.currency, 1.0)
        else:
            transaction.amount_mad = transaction.amount
            transaction.exchange_rate = 1.0
        
        db.session.commit()
        
        return jsonify({'message': 'Transaction updated successfully'})
    except Exception as e:
        print(f"Update transaction error: {e}")
        traceback.print_exc()
        db.session.rollback()
        return jsonify({'error': 'Failed to update transaction'}), 500

@app.route('/api/transactions/<int:transaction_id>', methods=['DELETE'])
@jwt_required()
def delete_transaction(transaction_id):
    try:
        user = get_user_from_token()
        if not user:
            return jsonify({'error': 'User not found'}), 404
            
        transaction = Transaction.query.get_or_404(transaction_id)
        
        if not can_access_company_data(user, transaction.company_id):
            return jsonify({'error': 'Access denied'}), 403
        
        db.session.delete(transaction)
        db.session.commit()
        
        return jsonify({'message': 'Transaction deleted successfully'})
    except Exception as e:
        print(f"Delete transaction error: {e}")
        traceback.print_exc()
        db.session.rollback()
        return jsonify({'error': 'Failed to delete transaction'}), 500

# ============ INVOICE ROUTES ============

@app.route('/api/invoices', methods=['GET'])
@jwt_required()
def get_invoices():
    try:
        user = get_user_from_token()
        if not user:
            return jsonify({'error': 'User not found'}), 404
            
        company_id = request.args.get('company_id', user.company_id)
        
        if not can_access_company_data(user, company_id):
            return jsonify({'error': 'Access denied'}), 403
        
        # Get query parameters
        status = request.args.get('status')
        
        query = Invoice.query.filter_by(company_id=company_id)
        
        if status:
            query = query.filter_by(status=status)
        
        invoices = query.order_by(Invoice.date_created.desc()).all()
        
        return jsonify([{
            'id': i.id,
            'company_id': i.company_id,
            'user_id': i.user_id,
            'invoice_number': i.invoice_number,
            'client_name': i.client_name,
            'client_email': i.client_email,
            'amount': i.amount,
            'currency': i.currency,
            'total_amount': i.total_amount,
            'date_created': i.date_created.isoformat(),
            'date_due': i.date_due.isoformat() if i.date_due else None,
            'status': i.status,
            'description': i.description
        } for i in invoices])
    except Exception as e:
        print(f"Get invoices error: {e}")
        traceback.print_exc()
        return jsonify({'error': 'Failed to get invoices'}), 500

@app.route('/api/invoices', methods=['POST'])
@jwt_required()
def create_invoice():
    try:
        user = get_user_from_token()
        if not user:
            return jsonify({'error': 'User not found'}), 404
            
        data = request.get_json()
        if not data:
            return jsonify({'error': 'No data provided'}), 400
        
        # Validate required fields
        required_fields = ['client_name', 'total_amount', 'date_created']
        for field in required_fields:
            if field not in data:
                return jsonify({'error': f'Missing required field: {field}'}), 400
        
        company_id = data.get('company_id', user.company_id)
        if not can_access_company_data(user, company_id):
            return jsonify({'error': 'Access denied'}), 403
        
        # Generate invoice number if not provided
        invoice_number = data.get('invoice_number')
        if not invoice_number:
            count = Invoice.query.filter_by(company_id=company_id).count()
            invoice_number = f"INV-{company_id:03d}-{count + 1:04d}"
        else:
            # Check if invoice number already exists
            existing = Invoice.query.filter_by(invoice_number=invoice_number).first()
            if existing:
                return jsonify({'error': 'Invoice number already exists'}), 400
        
        # Calculate due date
        date_created = datetime.strptime(data['date_created'], '%Y-%m-%d').date()
        date_due = date_created + timedelta(days=30)
        
        invoice = Invoice(
            company_id=company_id,
            user_id=user.id,
            invoice_number=invoice_number,
            client_name=data['client_name'],
            client_email=data.get('client_email', ''),
            amount=float(data['total_amount']),
            currency=data.get('currency', 'MAD'),
            total_amount=float(data['total_amount']),
            date_created=date_created,
            date_due=date_due,
            status=data.get('status', 'pending'),
            description=data.get('description', '')
        )
        
        db.session.add(invoice)
        db.session.commit()
        
        return jsonify({
            'message': 'Invoice created successfully',
            'id': invoice.id,
            'invoice_number': invoice_number
        }), 201
    except ValueError as e:
        return jsonify({'error': f'Invalid data format: {str(e)}'}), 400
    except Exception as e:
        print(f"Create invoice error: {e}")
        traceback.print_exc()
        db.session.rollback()
        return jsonify({'error': 'Failed to create invoice'}), 500

@app.route('/api/invoices/<int:invoice_id>', methods=['PUT'])
@jwt_required()
def update_invoice(invoice_id):
    try:
        user = get_user_from_token()
        if not user:
            return jsonify({'error': 'User not found'}), 404
            
        invoice = Invoice.query.get_or_404(invoice_id)
        
        if not can_access_company_data(user, invoice.company_id):
            return jsonify({'error': 'Access denied'}), 403
        
        data = request.get_json()
        if not data:
            return jsonify({'error': 'No data provided'}), 400
        
        # Update invoice fields
        if 'client_name' in data:
            invoice.client_name = data['client_name']
        if 'client_email' in data:
            invoice.client_email = data['client_email']
        if 'total_amount' in data:
            invoice.total_amount = float(data['total_amount'])
            invoice.amount = invoice.total_amount
        if 'status' in data:
            if data['status'] not in ['pending', 'paid', 'cancelled', 'overdue']:
                return jsonify({'error': 'Invalid status'}), 400
            invoice.status = data['status']
        if 'description' in data:
            invoice.description = data['description']
        
        db.session.commit()
        
        return jsonify({'message': 'Invoice updated successfully'})
    except Exception as e:
        print(f"Update invoice error: {e}")
        traceback.print_exc()
        db.session.rollback()
        return jsonify({'error': 'Failed to update invoice'}), 500

@app.route('/api/invoices/<int:invoice_id>', methods=['DELETE'])
@jwt_required()
def delete_invoice(invoice_id):
    try:
        user = get_user_from_token()
        if not user:
            return jsonify({'error': 'User not found'}), 404
            
        invoice = Invoice.query.get_or_404(invoice_id)
        
        if not can_access_company_data(user, invoice.company_id):
            return jsonify({'error': 'Access denied'}), 403
        
        db.session.delete(invoice)
        db.session.commit()
        
        return jsonify({'message': 'Invoice deleted successfully'})
    except Exception as e:
        print(f"Delete invoice error: {e}")
        traceback.print_exc()
        db.session.rollback()
        return jsonify({'error': 'Failed to delete invoice'}), 500

# ============ INVENTORY ROUTES ============

@app.route('/api/inventory', methods=['GET'])
@jwt_required()
def get_inventory():
    try:
        user = get_user_from_token()
        if not user:
            return jsonify({'error': 'User not found'}), 404
            
        company_id = request.args.get('company_id', user.company_id)
        
        if not can_access_company_data(user, company_id):
            return jsonify({'error': 'Access denied'}), 403
        
        query = InventoryItem.query.filter_by(company_id=company_id)
        items = query.all()
        
        return jsonify([{
            'id': i.id,
            'company_id': i.company_id,
            'user_id': i.user_id,
            'name': i.name,
            'category': i.category,
            'quantity': i.quantity,
            'unit_price': i.unit_price,
            'currency': i.currency,
            'unit_price_mad': i.unit_price_mad,
            'total_value': i.quantity * i.unit_price
        } for i in items])
    except Exception as e:
        print(f"Get inventory error: {e}")
        traceback.print_exc()
        return jsonify({'error': 'Failed to get inventory'}), 500

@app.route('/api/inventory', methods=['POST'])
@jwt_required()
def create_inventory_item():
    try:
        user = get_user_from_token()
        if not user:
            return jsonify({'error': 'User not found'}), 404
            
        data = request.get_json()
        if not data:
            return jsonify({'error': 'No data provided'}), 400
        
        # Validate required fields
        required_fields = ['name', 'quantity', 'unit_price']
        for field in required_fields:
            if field not in data:
                return jsonify({'error': f'Missing required field: {field}'}), 400
        
        company_id = data.get('company_id', user.company_id)
        if not can_access_company_data(user, company_id):
            return jsonify({'error': 'Access denied'}), 403
        
        currency = data.get('currency', 'MAD')
        unit_price = float(data['unit_price'])
        
        if unit_price < 0:
            return jsonify({'error': 'Unit price must be non-negative'}), 400
        
        quantity = int(data['quantity'])
        if quantity < 0:
            return jsonify({'error': 'Quantity must be non-negative'}), 400
        
        # Convert to MAD for storage consistency
        if currency != 'MAD':
            unit_price_mad = exchange_service.convert_currency(unit_price, currency, 'MAD')
        else:
            unit_price_mad = unit_price
        
        item = InventoryItem(
            company_id=company_id,
            user_id=user.id,
            name=data['name'],
            category=data.get('category', ''),
            quantity=quantity,
            unit_price=unit_price,
            currency=currency,
            unit_price_mad=unit_price_mad
        )
        
        db.session.add(item)
        db.session.commit()
        
        return jsonify({
            'message': 'Inventory item created successfully',
            'id': item.id
        }), 201
    except ValueError as e:
        return jsonify({'error': f'Invalid data format: {str(e)}'}), 400
    except Exception as e:
        print(f"Create inventory item error: {e}")
        traceback.print_exc()
        db.session.rollback()
        return jsonify({'error': 'Failed to create inventory item'}), 500

@app.route('/api/inventory/<int:item_id>', methods=['PUT'])
@jwt_required()
def update_inventory_item(item_id):
    try:
        user = get_user_from_token()
        if not user:
            return jsonify({'error': 'User not found'}), 404
            
        item = InventoryItem.query.get_or_404(item_id)
        
        if not can_access_company_data(user, item.company_id):
            return jsonify({'error': 'Access denied'}), 403
        
        data = request.get_json()
        if not data:
            return jsonify({'error': 'No data provided'}), 400
        
        # Update item fields
        if 'name' in data:
            item.name = data['name']
        if 'category' in data:
            item.category = data['category']
        if 'quantity' in data:
            item.quantity = int(data['quantity'])
        if 'unit_price' in data:
            item.unit_price = float(data['unit_price'])
        if 'currency' in data:
            item.currency = data['currency']
        
        # Recalculate MAD price
        if item.currency != 'MAD':
            item.unit_price_mad = exchange_service.convert_currency(item.unit_price, item.currency, 'MAD')
        else:
            item.unit_price_mad = item.unit_price
        
        db.session.commit()
        
        return jsonify({'message': 'Inventory item updated successfully'})
    except Exception as e:
        print(f"Update inventory item error: {e}")
        traceback.print_exc()
        db.session.rollback()
        return jsonify({'error': 'Failed to update inventory item'}), 500

@app.route('/api/inventory/<int:item_id>', methods=['DELETE'])
@jwt_required()
def delete_inventory_item(item_id):
    try:
        user = get_user_from_token()
        if not user:
            return jsonify({'error': 'User not found'}), 404
            
        item = InventoryItem.query.get_or_404(item_id)
        
        if not can_access_company_data(user, item.company_id):
            return jsonify({'error': 'Access denied'}), 403
        
        db.session.delete(item)
        db.session.commit()
        
        return jsonify({'message': 'Inventory item deleted successfully'})
    except Exception as e:
        print(f"Delete inventory item error: {e}")
        traceback.print_exc()
        db.session.rollback()
        return jsonify({'error': 'Failed to delete inventory item'}), 500

# ============ USER MANAGEMENT ROUTES ============

@app.route('/api/users', methods=['GET'])
@jwt_required()
def get_users():
    try:
        user = get_user_from_token()
        if not user:
            return jsonify({'error': 'User not found'}), 404
        
        # Only admins can access user management
        if not is_admin(user):
            return jsonify({'error': 'Admin access required'}), 403
        
        # Admins can see ALL users
        all_users = User.query.all()
        
        return jsonify([{
            'id': u.id,
            'name': u.name,
            'email': u.email,
            'role': u.role,
            'status': u.status,
            'company_id': u.company_id,
            'created_at': u.created_at.isoformat() if u.created_at else None,
            'last_login': u.last_login.isoformat() if u.last_login else None,
            'can_modify': True,
            'can_delete': u.role != 'admin' or u.id == user.id
        } for u in all_users])
    except Exception as e:
        print(f"Get users error: {e}")
        traceback.print_exc()
        return jsonify({'error': 'Failed to get users'}), 500

@app.route('/api/users', methods=['POST'])
@jwt_required()
def create_user():
    try:
        user = get_user_from_token()
        if not user:
            return jsonify({'error': 'User not found'}), 404
        
        if not is_admin(user):
            return jsonify({'error': 'Admin access required'}), 403
        
        data = request.get_json()
        if not data:
            return jsonify({'error': 'No data provided'}), 400
        
        # Validate required fields
        required_fields = ['name', 'email', 'password']
        for field in required_fields:
            if field not in data:
                return jsonify({'error': f'Missing required field: {field}'}), 400
        
        # Check for existing email
        existing_user = User.query.filter_by(email=data['email']).first()
        if existing_user:
            return jsonify({'error': 'Email already exists'}), 400
        
        new_user = User(
            name=data['name'],
            email=data['email'],
            role=data.get('role', 'user'),
            company_id=data.get('company_id', user.company_id),
            status=data.get('status', 'active')
        )
        new_user.set_password(data['password'])
        
        db.session.add(new_user)
        db.session.commit()
        
        return jsonify({
            'message': 'User created successfully',
            'id': new_user.id,
            'created_by_admin': user.name
        }), 201
    except Exception as e:
        print(f"Create user error: {e}")
        traceback.print_exc()
        db.session.rollback()
        return jsonify({'error': 'Failed to create user'}), 500

@app.route('/api/users/<int:user_id>', methods=['PUT'])
@jwt_required()
def update_user(user_id):
    try:
        current_user = get_user_from_token()
        if not current_user:
            return jsonify({'error': 'User not found'}), 404
        
        target_user = User.query.get_or_404(user_id)
        
        # Only admins can update users
        if not is_admin(current_user):
            return jsonify({'error': 'Admin access required'}), 403
        
        data = request.get_json()
        if not data:
            return jsonify({'error': 'No data provided'}), 400
        
        # Update user fields
        if 'name' in data:
            target_user.name = data['name']
        if 'email' in data:
            # Check if email already exists for another user
            existing = User.query.filter_by(email=data['email']).first()
            if existing and existing.id != user_id:
                return jsonify({'error': 'Email already exists'}), 400
            target_user.email = data['email']
        if 'role' in data:
            target_user.role = data['role']
        if 'status' in data:
            target_user.status = data['status']
        if 'company_id' in data:
            target_user.company_id = data['company_id']
        
        # Update password if provided
        if data.get('password'):
            target_user.set_password(data['password'])
        
        db.session.commit()
        
        return jsonify({
            'message': 'User updated successfully',
            'updated_by_admin': current_user.name
        })
    except Exception as e:
        print(f"Update user error: {e}")
        traceback.print_exc()
        db.session.rollback()
        return jsonify({'error': 'Failed to update user'}), 500

@app.route('/api/users/<int:user_id>', methods=['DELETE'])
@jwt_required()
def delete_user(user_id):
    try:
        current_user = get_user_from_token()
        if not current_user:
            return jsonify({'error': 'User not found'}), 404
        
        target_user = User.query.get_or_404(user_id)
        
        # Check permissions
        if not is_admin(current_user):
            return jsonify({'error': 'Admin access required'}), 403
        
        # Prevent deleting other admins
        if is_admin(target_user) and target_user.id != current_user.id:
            return jsonify({'error': 'Cannot delete other administrator accounts'}), 403
        
        # Prevent admin from deleting themselves
        if target_user.id == current_user.id:
            return jsonify({'error': 'Cannot delete your own account'}), 403
        
        db.session.delete(target_user)
        db.session.commit()
        
        return jsonify({
            'message': 'User deleted successfully',
            'deleted_by_admin': current_user.name
        })
    except Exception as e:
        print(f"Delete user error: {e}")
        traceback.print_exc()
        db.session.rollback()
        return jsonify({'error': 'Failed to delete user'}), 500

@app.route('/api/users/<int:user_id>/view', methods=['GET'])
@jwt_required()
def view_user_data(user_id):
    try:
        current_user = get_user_from_token()
        if not current_user:
            return jsonify({'error': 'User not found'}), 404
        
        # Only admins can view user data
        if not is_admin(current_user):
            return jsonify({'error': 'Admin access required'}), 403
        
        target_user = User.query.get_or_404(user_id)
        company_id = target_user.company_id
        
        # Get user's data
        transactions = Transaction.query.filter_by(company_id=company_id).limit(100).all()
        invoices = Invoice.query.filter_by(company_id=company_id).limit(50).all()
        inventory = InventoryItem.query.filter_by(company_id=company_id).limit(50).all()
        data_entries = DataEntry.query.filter_by(company_id=company_id).limit(50).all()
        
        return jsonify({
            'user': {
                'id': target_user.id,
                'name': target_user.name,
                'email': target_user.email,
                'role': target_user.role,
                'company_id': target_user.company_id,
                'status': target_user.status,
                'created_at': target_user.created_at.isoformat() if target_user.created_at else None,
                'last_login': target_user.last_login.isoformat() if target_user.last_login else None
            },
            'summary': {
                'transactions_count': len(transactions),
                'invoices_count': len(invoices),
                'inventory_count': len(inventory),
                'data_entries_count': len(data_entries)
            },
            'transactions': [{
                'id': t.id,
                'date': t.date.isoformat(),
                'description': t.description,
                'amount': t.amount,
                'currency': t.currency or 'MAD',
                'type': t.type,
                'category': t.category or ''
            } for t in transactions[:20]],
            'invoices': [{
                'id': i.id,
                'invoice_number': i.invoice_number,
                'client_name': i.client_name,
                'total_amount': i.total_amount,
                'status': i.status,
                'date_created': i.date_created.isoformat()
            } for i in invoices[:20]],
            'access_info': {
                'viewed_by_admin': current_user.name,
                'view_timestamp': datetime.now().isoformat()
            }
        })
    except Exception as e:
        print(f"View user data error: {e}")
        traceback.print_exc()
        return jsonify({'error': 'Failed to get user data'}), 500

# ============ DATA ENTRY ROUTES ============

@app.route('/api/data-entries', methods=['GET'])
@jwt_required()
def get_data_entries():
    try:
        user = get_user_from_token()
        if not user:
            return jsonify({'error': 'User not found'}), 404
            
        company_id = request.args.get('company_id', user.company_id)
        entry_type = request.args.get('type')
        
        if not can_access_company_data(user, company_id):
            return jsonify({'error': 'Access denied'}), 403
        
        query = DataEntry.query.filter_by(company_id=company_id)
        
        # Non-admin users can only see their own entries
        if not is_admin(user):
            query = query.filter_by(user_id=user.id)
        
        if entry_type:
            query = query.filter_by(entry_type=entry_type)
        
        entries = query.order_by(DataEntry.created_at.desc()).all()
        
        return jsonify([{
            'id': e.id,
            'company_id': e.company_id,
            'user_id': e.user_id,
            'entry_type': e.entry_type,
            'title': e.title,
            'description': e.description,
            'data': json.loads(e.data) if e.data else [],
            'status': e.status,
            'created_by': e.created_by,
            'created_at': e.created_at.isoformat() if e.created_at else None,
            'updated_at': e.updated_at.isoformat() if e.updated_at else None
        } for e in entries])
    except Exception as e:
        print(f"Get data entries error: {e}")
        traceback.print_exc()
        return jsonify({'error': 'Failed to get data entries'}), 500

@app.route('/api/data-entries', methods=['POST'])
@jwt_required()
def create_data_entry():
    try:
        user = get_user_from_token()
        if not user:
            return jsonify({'error': 'User not found'}), 404
            
        data = request.get_json()
        if not data:
            return jsonify({'error': 'No data provided'}), 400
        
        # Validate required fields
        if 'entry_type' not in data:
            return jsonify({'error': 'Missing required field: entry_type'}), 400
        
        company_id = data.get('company_id', user.company_id)
        if not can_access_company_data(user, company_id):
            return jsonify({'error': 'Access denied'}), 403
        
        entry = DataEntry(
            company_id=company_id,
            user_id=user.id,
            entry_type=data['entry_type'],
            title=data.get('title', ''),
            description=data.get('description', ''),
            data=json.dumps(data.get('data', [])),
            status=data.get('status', 'active'),
            created_by=user.id
        )
        
        db.session.add(entry)
        db.session.commit()
        
        return jsonify({
            'message': 'Data entry created successfully',
            'id': entry.id
        }), 201
    except Exception as e:
        print(f"Create data entry error: {e}")
        traceback.print_exc()
        db.session.rollback()
        return jsonify({'error': 'Failed to create data entry'}), 500

@app.route('/api/data-entries/<int:entry_id>', methods=['PUT'])
@jwt_required()
def update_data_entry(entry_id):
    try:
        user = get_user_from_token()
        if not user:
            return jsonify({'error': 'User not found'}), 404
            
        entry = DataEntry.query.get_or_404(entry_id)
        
        if not can_access_company_data(user, entry.company_id):
            return jsonify({'error': 'Access denied'}), 403
        
        # Non-admin users can only update their own entries
        if not is_admin(user) and entry.user_id != user.id:
            return jsonify({'error': 'Access denied - can only update own entries'}), 403
        
        data = request.get_json()
        if not data:
            return jsonify({'error': 'No data provided'}), 400
        
        if 'title' in data:
            entry.title = data['title']
        if 'description' in data:
            entry.description = data['description']
        if 'data' in data:
            entry.data = json.dumps(data['data'])
        if 'status' in data:
            entry.status = data['status']
        
        entry.updated_at = datetime.utcnow()
        
        db.session.commit()
        
        return jsonify({'message': 'Data entry updated successfully'})
    except Exception as e:
        print(f"Update data entry error: {e}")
        traceback.print_exc()
        db.session.rollback()
        return jsonify({'error': 'Failed to update data entry'}), 500

@app.route('/api/data-entries/<int:entry_id>', methods=['DELETE'])
@jwt_required()
def delete_data_entry(entry_id):
    try:
        user = get_user_from_token()
        if not user:
            return jsonify({'error': 'User not found'}), 404
            
        entry = DataEntry.query.get_or_404(entry_id)
        
        if not can_access_company_data(user, entry.company_id):
            return jsonify({'error': 'Access denied'}), 403
        
        # Non-admin users can only delete their own entries
        if not is_admin(user) and entry.user_id != user.id:
            return jsonify({'error': 'Access denied - can only delete own entries'}), 403
        
        db.session.delete(entry)
        db.session.commit()
        
        return jsonify({'message': 'Data entry deleted successfully'})
    except Exception as e:
        print(f"Delete data entry error: {e}")
        traceback.print_exc()
        db.session.rollback()
        return jsonify({'error': 'Failed to delete data entry'}), 500

# ============ AI ROUTES ============

@app.route('/api/ai/insights', methods=['GET'])
@jwt_required()
def get_ai_insights():
    try:
        user = get_user_from_token()
        if not user:
            return jsonify({'error': 'User not found'}), 404
        
        company_id = request.args.get('company_id', user.company_id)
        if not can_access_company_data(user, company_id):
            return jsonify({'error': 'Access denied'}), 403

        # Get last 90 days of transactions
        start_date = date.today() - timedelta(days=90)
        transactions = Transaction.query.filter(
            Transaction.company_id == company_id,
            Transaction.date >= start_date
        ).all()

        # Detect anomalies
        anomalies = detect_anomalies(transactions) if AI_AVAILABLE else []

        # Get income for forecasting
        income_tx = [t for t in transactions if t.type == 'income']
        amounts = [t.amount_mad for t in income_tx]
        dates = [t.date for t in income_tx]
        forecast = forecast_cash_flow(dates, amounts, steps=30) if AI_AVAILABLE else [0] * 30

        # Get spending insights
        insights = get_spending_insights(transactions, period_days=30) if AI_AVAILABLE else {}

        return jsonify({
            "anomalies": anomalies,
            "anomaly_count": len(anomalies),
            "forecast_next_30_days_avg": round(sum(forecast) / len(forecast), 2) if forecast else 0,
            "spending_insights": insights,
            "ai_enabled": AI_AVAILABLE,
            "data_period": "90 days"
        })
    except Exception as e:
        print(f"AI insights error: {e}")
        traceback.print_exc()
        return jsonify({
            "anomalies": [],
            "anomaly_count": 0,
            "forecast_next_30_days_avg": 0,
            "spending_insights": {},
            "ai_enabled": AI_AVAILABLE,
            "error": "AI insights temporarily unavailable"
        }), 200

@app.route('/api/ai/categorize', methods=['POST'])
@jwt_required()
def ai_categorize():
    try:
        data = request.get_json()
        if not data:
            return jsonify({'error': 'No data provided'}), 400
        
        description = data.get('description', '')
        category = categorize_transaction(description) if AI_AVAILABLE else "Other"
        
        return jsonify({
            "category": category,
            "description": description,
            "ai_enabled": AI_AVAILABLE
        })
    except Exception as e:
        print(f"Categorize error: {e}")
        return jsonify({"category": "Other", "ai_enabled": AI_AVAILABLE}), 200

# ============ FILE IMPORT ROUTE ============

def allowed_file(filename):
    """Check if file extension is allowed"""
    return '.' in filename and \
           filename.rsplit('.', 1)[1].lower() in Config.ALLOWED_EXTENSIONS

def process_csv_file(file_content):
    """Process CSV file and return structured data"""
    try:
        # Try UTF-8 first
        try:
            content = file_content.decode('utf-8')
        except UnicodeDecodeError:
            # Fallback to latin-1
            content = file_content.decode('latin-1')
        
        # Parse CSV
        csv_reader = csv.DictReader(io.StringIO(content))
        data = list(csv_reader)
        
        # Clean data
        cleaned_data = []
        for row in data:
            cleaned_row = {}
            for key, value in row.items():
                if value and value.strip():
                    cleaned_row[key.strip()] = value.strip()
            if cleaned_row:
                cleaned_data.append(cleaned_row)
        
        return cleaned_data, None
    except Exception as e:
        return None, str(e)

def process_excel_file(file_content):
    """Process Excel file and return structured data"""
    if not PANDAS_AVAILABLE:
        return None, "Excel import not available. Install pandas and openpyxl."
    
    try:
        df = pd.read_excel(io.BytesIO(file_content))
        
        # Convert DataFrame to list of dictionaries
        data = df.to_dict('records')
        
        # Clean data
        cleaned_data = []
        for row in data:
            cleaned_row = {}
            for key, value in row.items():
                if pd.notna(value):
                    cleaned_row[str(key)] = str(value) if not isinstance(value, (int, float)) else value
            if cleaned_row:
                cleaned_data.append(cleaned_row)
        
        return cleaned_data, None
    except Exception as e:
        return None, str(e)

@app.route('/api/import-csv', methods=['POST'])
@jwt_required()
def import_csv():
    try:
        user = get_user_from_token()
        if not user:
            return jsonify({'error': 'User not found'}), 404
        
        if 'file' not in request.files:
            return jsonify({'error': 'No file uploaded'}), 400
        
        file = request.files['file']
        if not file.filename or not allowed_file(file.filename):
            return jsonify({'error': 'Invalid file type. Allowed: CSV, XLSX, XLS, JSON'}), 400
        
        # Get file content
        file_content = file.read()
        file_type = file.filename.rsplit('.', 1)[1].lower()
        
        # Process file based on type
        if file_type == 'csv':
            data, error = process_csv_file(file_content)
        elif file_type in ['xlsx', 'xls']:
            data, error = process_excel_file(file_content)
        elif file_type == 'json':
            try:
                data = json.loads(file_content.decode('utf-8'))
                error = None
            except Exception as e:
                data = None
                error = str(e)
        else:
            return jsonify({'error': 'Unsupported file type'}), 400
        
        if error:
            return jsonify({'error': f'Failed to process file: {error}'}), 400
        
        if not data:
            return jsonify({'error': 'No data found in file'}), 400
        
        # Import transactions
        imported_count = 0
        errors = []
        
        for row_num, row in enumerate(data, start=2):
            try:
                # Flexible field mapping (case-insensitive)
                row_lower = {k.lower(): v for k, v in row.items()}
                
                date_field = row_lower.get('date')
                desc_field = row_lower.get('description')
                amount_field = row_lower.get('amount')
                type_field = row_lower.get('type')
                category_field = row_lower.get('category', '')
                currency_field = row_lower.get('currency', 'MAD')
                
                if not all([date_field, desc_field, amount_field, type_field]):
                    errors.append(f"Row {row_num}: Missing required fields (date, description, amount, type)")
                    continue
                
                # Clean and validate data
                amount = float(str(amount_field).replace(',', '').replace(' ', ''))
                transaction_type = str(type_field).lower().strip()
                currency = str(currency_field).upper().strip()
                
                if transaction_type not in ['income', 'expense']:
                    errors.append(f"Row {row_num}: Type must be 'income' or 'expense'")
                    continue
                
                if amount <= 0:
                    errors.append(f"Row {row_num}: Amount must be positive")
                    continue
                
                # Convert currency
                if currency != 'MAD':
                    amount_mad = exchange_service.convert_currency(amount, currency, 'MAD')
                    rates = exchange_service.get_live_rates('MAD')
                    exchange_rate = 1.0 / rates.get(currency, 1.0)
                else:
                    amount_mad = amount
                    exchange_rate = 1.0
                
                # Parse date
                try:
                    if isinstance(date_field, str):
                        # Try multiple date formats
                        for fmt in ['%Y-%m-%d', '%d/%m/%Y', '%m/%d/%Y', '%Y/%m/%d']:
                            try:
                                transaction_date = datetime.strptime(date_field, fmt).date()
                                break
                            except ValueError:
                                continue
                        else:
                            transaction_date = date.today()
                    else:
                        if PANDAS_AVAILABLE:
                            transaction_date = pd.to_datetime(date_field).date()
                        else:
                            transaction_date = date.today()
                except:
                    transaction_date = date.today()
                
                # Auto-categorize if no category
                if not category_field and AI_AVAILABLE:
                    category_field = categorize_transaction(str(desc_field))
                
                # Create transaction
                transaction = Transaction(
                    company_id=user.company_id,
                    user_id=user.id,
                    date=transaction_date,
                    description=str(desc_field)[:500],
                    amount=amount,
                    currency=currency,
                    original_currency=currency,
                    amount_mad=amount_mad,
                    exchange_rate=exchange_rate,
                    type=transaction_type,
                    category=str(category_field)[:100] if category_field else '',
                    source='csv_import'
                )
                db.session.add(transaction)
                imported_count += 1
                
            except Exception as e:
                errors.append(f"Row {row_num}: {str(e)}")
        
        if imported_count > 0:
            db.session.commit()
        
        return jsonify({
            'message': 'File imported successfully',
            'imported_count': imported_count,
            'total_rows': len(data),
            'error_count': len(errors),
            'errors': errors[:10]  # Return first 10 errors
        })
        
    except Exception as e:
        print(f"Import CSV error: {e}")
        traceback.print_exc()
        db.session.rollback()
        return jsonify({'error': f'Failed to process file: {str(e)}'}), 500

# ============ COMPANY ROUTES ============

@app.route('/api/companies', methods=['GET'])
@jwt_required()
def get_companies():
    try:
        user = get_user_from_token()
        if not user:
            return jsonify({'error': 'User not found'}), 404
        
        # Only admins can see all companies
        if is_admin(user):
            companies = Company.query.all()
        else:
            # Regular users only see their company
            companies = Company.query.filter_by(id=user.company_id).all()
        
        return jsonify([{
            'id': c.id,
            'name': c.name,
            'address': c.address,
            'phone': c.phone,
            'email': c.email,
            'tax_id': c.tax_id,
            'base_currency': c.base_currency,
            'status': c.status
        } for c in companies])
    except Exception as e:
        print(f"Get companies error: {e}")
        traceback.print_exc()
        return jsonify({'error': 'Failed to get companies'}), 500

# ============ DATABASE INITIALIZATION ============

def init_db():
    """Initialize database with tables and default data"""
    try:
        with app.app_context():
            # Create all tables
            db.create_all()
            
            # Check if already initialized
            if Company.query.first():
                print("✅ Database already initialized")
                return
            
            # Create default company
            company = Company(
                name=Config.COMPANY_NAME,
                address=Config.COMPANY_ADDRESS,
                phone=Config.COMPANY_PHONE,
                email=Config.COMPANY_EMAIL,
                base_currency='MAD',
                status='active'
            )
            db.session.add(company)
            db.session.flush()
            
            # Create default admin user
            admin_user = User(
                name='Admin User',
                email='admin@hdtransit.com',
                role='admin',
                company_id=company.id,
                status='active'
            )
            admin_user.set_password('admin123')
            db.session.add(admin_user)
            
            # Create a regular user for testing
            regular_user = User(
                name='User Test',
                email='user@hdtransit.com',
                role='user',
                company_id=company.id,
                status='active'
            )
            regular_user.set_password('user123')
            db.session.add(regular_user)
            
            db.session.commit()
            
            print("✅ Database initialized successfully!")
            print(f"🏢 Company: {company.name}")
            print(f"👤 Admin User: admin@hdtransit.com / admin123")
            print(f"👤 Regular User: user@hdtransit.com / user123")
            print(f"💰 Base Currency: {company.base_currency}")
            
    except Exception as e:
        print(f"❌ Database initialization failed: {e}")
        traceback.print_exc()
        db.session.rollback()

# ============ APPLICATION STARTUP ============

if __name__ == '__main__':
    # Initialize database
    init_db()
    
    # Run the application
    print("=" * 60)
    print("🚀 Starting Happy Deal Transit ERP Backend v3.3-STABLE")
    print("=" * 60)
    print(f"✓ AI Features: {'Enabled' if AI_AVAILABLE else 'Disabled'}")
    print(f"✓ Pandas Support: {'Enabled' if PANDAS_AVAILABLE else 'Disabled'}")
    print("=" * 60)
    print("📊 Available Endpoints:")
    print("   Authentication:")
    print("     - POST /api/login")
    print("     - GET  /api/user/profile")
    print("   Dashboard:")
    print("     - GET  /api/dashboard")
    print("     - GET  /api/dashboard/charts")
    print("   Transactions:")
    print("     - GET/POST/PUT/DELETE /api/transactions")
    print("     - POST /api/transactions/bulk-import")
    print("   Invoices:")
    print("     - GET/POST/PUT/DELETE /api/invoices")
    print("   Inventory:")
    print("     - GET/POST/PUT/DELETE /api/inventory")
    print("   Users (Admin Only):")
    print("     - GET/POST/PUT/DELETE /api/users")
    print("     - GET  /api/users/<id>/view")
    print("   Data Entries:")
    print("     - GET/POST/PUT/DELETE /api/data-entries")
    print("   AI & Analytics:")
    print("     - GET  /api/ai/insights")
    print("     - POST /api/ai/categorize")
    print("   Utilities:")
    print("     - GET  /api/exchange-rates")
    print("     - POST /api/import-csv")
    print("     - GET  /api/companies")
    print("     - GET  /api/test")
    print("     - GET  /api/health")
    print("=" * 60)
    print("🌐 CORS enabled for localhost:3000")
    print("🔐 JWT authentication required for most endpoints")
    print("🤖 AI categorization with fallback enabled")
    print("💱 Multi-currency support with real-time rates")
    print("📈 Dashboard analytics with charts")
    print("👥 Role-based access control (Admin/User)")
    print("📄 File import support (CSV/Excel/JSON)")
    print("🗃️ Data entry system")
    print("=" * 60)
    
    app.run(
        host='0.0.0.0', 
        port=5000, 
        debug=True,
        threaded=True
    )
