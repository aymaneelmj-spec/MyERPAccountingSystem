// src/services/api.js - COMPLETE FIXED VERSION

class ApiService {
  constructor() {
    this.baseURL = 'http://localhost:5000/api';
    this.token = null;
  }

  init() {
    if (typeof window !== 'undefined') {
      this.token = localStorage.getItem('token');
    }
  }

  setToken(token) {
    this.token = token;
    if (typeof window !== 'undefined') {
      localStorage.setItem('token', token);
    }
  }

  getToken() {
    if (typeof window !== 'undefined') {
      return this.token || localStorage.getItem('token');
    }
    return this.token;
  }

  clearToken() {
    this.token = null;
    if (typeof window !== 'undefined') {
      localStorage.removeItem('token');
    }
  }

  async apiCall(endpoint, method = 'GET', data = null) {
    const url = `${this.baseURL}${endpoint}`;
    console.log(`🚀 API Call: ${method} ${url}`);

    const headers = {
      'Content-Type': 'application/json'
    };

    const token = this.getToken();
    if (token) {
      headers['Authorization'] = `Bearer ${token}`;
    }

    const config = {
      method,
      headers,
      mode: 'cors'
    };

    if (data && (method === 'POST' || method === 'PUT')) {
      config.body = JSON.stringify(data);
    }

    try {
      const response = await fetch(url, config);
      console.log(`📡 Response Status: ${response.status} ${response.statusText}`);

      if (response.status === 401) {
        this.clearToken();
        throw new Error('Session expired. Please login again.');
      }

      if (!response.ok) {
        let errorMessage = `HTTP ${response.status}`;
        try {
          const errorData = await response.json();
          errorMessage = errorData.error || errorData.message || errorMessage;
        } catch (e) {
          errorMessage = response.statusText || errorMessage;
        }
        throw new Error(errorMessage);
      }

      const responseData = await response.json();
      console.log('✅ API Response received');
      return responseData;

    } catch (error) {
      console.error(`❌ API Error [${endpoint}]:`, error);
      throw error;
    }
  }

  // === AUTHENTICATION ===
  async login(credentials) {
    try {
      const response = await this.apiCall('/login', 'POST', credentials);
      if (response.access_token) {
        this.setToken(response.access_token);
        console.log('✅ Login successful, token stored');
      }
      return response;
    } catch (error) {
      console.error('❌ Login failed:', error);
      throw error;
    }
  }

  async getUserProfile() {
    return await this.apiCall('/user/profile');
  }

  // === EXCHANGE RATES ===
  async updateExchangeRates() {
    console.log('💱 Updating exchange rates...');
    try {
      const response = await this.apiCall('/exchange-rates');
      return response.rates || {
        MAD: 1,
        USD: 10.12,
        EUR: 11.05,
        GBP: 12.78
      };
    } catch (error) {
      console.error('❌ Failed to update exchange rates:', error);
      return {
        MAD: 1,
        USD: 10.12,
        EUR: 11.05,
        GBP: 12.78
      };
    }
  }

  // === DASHBOARD ===
  async getDashboardStats(companyId, currency = 'MAD') {
    return await this.apiCall(`/dashboard?company_id=${companyId}&currency=${currency}`);
  }

  async getChartData(companyId, period = '6months', currency = 'MAD') {
    return await this.apiCall(`/dashboard/charts?company_id=${companyId}&period=${period}&currency=${currency}`);
  }

  // === AI FEATURES ===
  async getAIInsights(companyId) {
    try {
      console.log('🧠 Fetching AI insights...');
      return await this.apiCall(`/ai/insights?company_id=${companyId}`);
    } catch (error) {
      console.warn('⚠️ AI insights not available:', error.message);
      return {
        anomalies: [],
        forecast_next_30_days_avg: 0,
        ai_enabled: false,
        message: 'AI features not available in this version'
      };
    }
  }

  async categorizeTransaction(description) {
    try {
      const response = await this.apiCall('/ai/categorize', 'POST', { description });
      return response.category || 'Other';
    } catch (error) {
      console.warn('⚠️ AI categorization failed, using fallback');
      return 'Other';
    }
  }

  // === TRANSACTIONS ===
  async getTransactions(companyId) {
    try {
      return await this.apiCall(`/transactions?company_id=${companyId}`);
    } catch (error) {
      console.error('❌ Failed to get transactions:', error);
      throw error;
    }
  }

  async createTransaction(data) {
    if (!data.category && data.description) {
      try {
        data.category = await this.categorizeTransaction(data.description);
        console.log('🤖 Auto-categorized as:', data.category);
      } catch (e) {
        data.category = 'Other';
      }
    }
    return await this.apiCall('/transactions', 'POST', data);
  }

  async updateTransaction(id, data) {
    return await this.apiCall(`/transactions/${id}`, 'PUT', data);
  }

  async deleteTransaction(id) {
    return await this.apiCall(`/transactions/${id}`, 'DELETE');
  }

  async bulkImportTransactions(transactions) {
    return await this.apiCall('/transactions/bulk-import', 'POST', transactions);
  }

  // === INVOICES ===
  async getInvoices(companyId) {
    try {
      return await this.apiCall(`/invoices?company_id=${companyId}`);
    } catch (error) {
      console.error('❌ Failed to get invoices:', error);
      throw error;
    }
  }

  async createInvoice(data) {
    return await this.apiCall('/invoices', 'POST', data);
  }

  async updateInvoice(id, data) {
    return await this.apiCall(`/invoices/${id}`, 'PUT', data);
  }

  async deleteInvoice(id) {
    return await this.apiCall(`/invoices/${id}`, 'DELETE');
  }

  async bulkImportInvoices(invoices) {
    return await this.apiCall('/invoices/bulk-import', 'POST', invoices);
  }

  // === INVENTORY ===
  async getInventory(companyId) {
    try {
      return await this.apiCall(`/inventory?company_id=${companyId}`);
    } catch (error) {
      console.error('❌ Failed to get inventory:', error);
      throw error;
    }
  }

  async createInventoryItem(data) {
    return await this.apiCall('/inventory', 'POST', data);
  }

  async updateInventoryItem(id, data) {
    return await this.apiCall(`/inventory/${id}`, 'PUT', data);
  }

  async deleteInventoryItem(id) {
    return await this.apiCall(`/inventory/${id}`, 'DELETE');
  }

  async bulkImportInventory(items) {
    return await this.apiCall('/inventory/bulk-import', 'POST', items);
  }

  // === DATA ENTRIES ===
  async getDataEntries(companyId) {
    try {
      return await this.apiCall(`/data-entries?company_id=${companyId}`);
    } catch (error) {
      console.error('❌ Failed to get data entries:', error);
      throw error;
    }
  }

  async createDataEntry(data) {
    return await this.apiCall('/data-entries', 'POST', data);
  }

  async updateDataEntry(id, data) {
    return await this.apiCall(`/data-entries/${id}`, 'PUT', data);
  }

  async deleteDataEntry(id) {
    return await this.apiCall(`/data-entries/${id}`, 'DELETE');
  }

  // === USER MANAGEMENT (Admin only) ===
  async getUsers() {
    try {
      return await this.apiCall('/users');
    } catch (error) {
      console.error('❌ Failed to get users:', error);
      throw error;
    }
  }

  async createUser(data) {
    return await this.apiCall('/users', 'POST', data);
  }

  async updateUser(id, data) {
    return await this.apiCall(`/users/${id}`, 'PUT', data);
  }

  async deleteUser(id) {
    return await this.apiCall(`/users/${id}`, 'DELETE');
  }

  async getUserData(userId) {
    return await this.apiCall(`/users/${userId}/view`);
  }

  // === COMPANIES ===
  async getCompanies() {
    try {
      return await this.apiCall('/companies');
    } catch (error) {
      console.error('❌ Failed to get companies:', error);
      throw error;
    }
  }

  // === FILE IMPORT ===
  async uploadFile(file, importType) {
    const formData = new FormData();
    formData.append('file', file);
    formData.append('import_type', importType);

    const token = this.getToken();
    const headers = {};
    if (token) {
      headers['Authorization'] = `Bearer ${token}`;
    }

    try {
      const response = await fetch(`${this.baseURL}/import-csv`, {
        method: 'POST',
        headers,
        body: formData,
        mode: 'cors'
      });

      if (!response.ok) {
        const errorData = await response.json();
        throw new Error(errorData.error || 'Upload failed');
      }

      return await response.json();
    } catch (error) {
      console.error('❌ File upload error:', error);
      throw error;
    }
  }

  // Health check
  async performHealthCheck() {
    console.log('🏥 Performing health check...');
    try {
      const response = await this.apiCall('/test');
      console.log('📊 API Health Status:', {
        'Base URL': this.baseURL,
        'Connected': '✅',
        'Has Token': this.getToken() ? '✅' : '❌',
        'Network': '✅',
        'Last Check': new Date().toLocaleString('fr-FR')
      });
      return response;
    } catch (error) {
      console.error('❌ API Health Check Failed:', error);
      if (error.message.includes('Failed to fetch')) {
        console.warn('🚧 Backend server might not be running on localhost:5000');
        console.warn('🚧 Make sure your Flask/Python backend is started');
      }
      throw error;
    }
  }
}

// Create and export singleton instance
const apiService = new ApiService();

if (typeof window !== 'undefined') {
  apiService.init();
  setTimeout(() => {
    apiService.performHealthCheck().catch(e => {
      console.warn('⚠️ Health check failed (normal if backend offline):', e.message);
    });
  }, 1000);
}

const authService = {
  login: (credentials) => apiService.login(credentials),
  logout: () => apiService.clearToken(),
  getUserProfile: () => apiService.getUserProfile(),
  isAuthenticated: () => !!apiService.getToken()
};

export { apiService, authService };
export default apiService;