import React, { useState, useEffect } from 'react';
import { BrowserRouter as Router, Routes, Route, Navigate } from 'react-router-dom';
import { Toaster } from 'react-hot-toast';
import Login from './components/Login';
import Layout from './components/Layout';
import Dashboard from './components/Dashboard';
import Transactions from './components/Transactions';
import Invoices from './components/Invoices';
import Inventory from './components/Inventory';
import Users from './components/Users';
import Reports from './components/Reports';
import Profile from './components/Profile';
import ChangePassword from './components/ChangePassword';
import DataEntry from './components/DataEntry';
import { authService, apiService } from './services/api';

function App() {
  const [isAuthenticated, setIsAuthenticated] = useState(false);
  const [user, setUser] = useState(null);
  const [isLoading, setIsLoading] = useState(true);

  useEffect(() => {
    checkAuthStatus();
  }, []);

  const checkAuthStatus = async () => {
    const token = localStorage.getItem('token');
    if (token) {
      try {
        const userData = await authService.getUserProfile();
        setUser(userData);
        setIsAuthenticated(true);
      } catch (error) {
        console.error('Auth check failed:', error);
        localStorage.removeItem('token');
      }
    }
    setIsLoading(false);
  };

  const handleLogin = async (credentials) => {
    try {
      const response = await authService.login(credentials);
      setUser(response.user);
      setIsAuthenticated(true);
      return response;
    } catch (error) {
      throw error;
    }
  };

  const handleLogout = () => {
    authService.logout();
    setIsAuthenticated(false);
    setUser(null);
  };

  if (isLoading) {
    return (
      <div className="min-h-screen flex items-center justify-center bg-gradient-to-br from-blue-50 to-indigo-100">
        <div className="text-center">
          <div className="animate-spin rounded-full h-32 w-32 border-b-2 border-blue-600 mx-auto"></div>
          <p className="mt-4 text-gray-600">Chargement du système ERP...</p>
        </div>
      </div>
    );
  }

  if (!isAuthenticated) {
    return (
      <>
        <Login onLogin={handleLogin} />
        <Toaster 
          position="top-right"
          toastOptions={{
            duration: 4000,
            style: {
              background: '#363636',
              color: '#fff',
            },
            success: {
              style: {
                background: '#059669',
              },
            },
            error: {
              style: {
                background: '#DC2626',
              },
            },
          }}
        />
      </>
    );
  }

  return (
    <Router>
      <div className="App">
        <Routes>
          <Route path="/login" element={<Navigate to="/" replace />} />
          <Route 
            path="/" 
            element={
              <Layout user={user} onLogout={handleLogout}>
                <Dashboard user={user} apiService={apiService} />
              </Layout>
            } 
          />
          <Route 
            path="/transactions" 
            element={
              <Layout user={user} onLogout={handleLogout}>
                <Transactions user={user} apiService={apiService} />
              </Layout>
            } 
          />
          <Route 
            path="/invoices" 
            element={
              <Layout user={user} onLogout={handleLogout}>
                <Invoices user={user} apiService={apiService} />
              </Layout>
            } 
          />
          <Route 
            path="/inventory" 
            element={
              <Layout user={user} onLogout={handleLogout}>
                <Inventory user={user} apiService={apiService} />
              </Layout>
            } 
          />
          <Route 
            path="/data-entry" 
            element={
              <Layout user={user} onLogout={handleLogout}>
                <DataEntry user={user} apiService={apiService} />
              </Layout>
            } 
          />
          <Route 
            path="/users" 
            element={
              <Layout user={user} onLogout={handleLogout}>
                <Users user={user} apiService={apiService} />
              </Layout>
            } 
          />
          <Route 
            path="/reports" 
            element={
              <Layout user={user} onLogout={handleLogout}>
                <Reports user={user} apiService={apiService} />
              </Layout>
            } 
          />
          <Route 
            path="/profile" 
            element={
              <Layout user={user} onLogout={handleLogout}>
                <Profile user={user} apiService={apiService} />
              </Layout>
            } 
          />
          <Route 
            path="/change-password" 
            element={
              <Layout user={user} onLogout={handleLogout}>
                <ChangePassword apiService={apiService} />
              </Layout>
            } 
          />
          <Route path="*" element={<Navigate to="/" replace />} />
        </Routes>
        <Toaster 
          position="top-right"
          toastOptions={{
            duration: 4000,
            style: {
              background: '#363636',
              color: '#fff',
            },
            success: {
              style: {
                background: '#059669',
              },
            },
            error: {
              style: {
                background: '#DC2626',
              },
            },
          }}
        />
      </div>
    </Router>
  );
}

export default App;