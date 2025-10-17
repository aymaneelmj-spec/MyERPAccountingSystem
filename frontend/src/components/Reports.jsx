// src/components/Reports.jsx - Fixed Encoding
import React, { useState } from 'react';
import { Download, Calendar, FileText, TrendingUp, Folder } from 'lucide-react';
import { toast } from 'react-hot-toast';
import { apiService } from '../services/api';

const Reports = ({ user, companyId }) => {
  const [loading, setLoading] = useState(false);
  const [selectedCurrency, setSelectedCurrency] = useState('MAD');

  const generateComprehensiveReport = async (reportType) => {
    setLoading(true);
    try {
      const actualCompanyId = user?.company_id || companyId || 1;
      const [stats, charts, transactions, invoices, inventory] = await Promise.all([
        apiService.getDashboardStats(actualCompanyId, selectedCurrency).catch(() => ({})),
        apiService.getChartData(actualCompanyId, reportType, selectedCurrency).catch(() => ({})),
        apiService.getTransactions(actualCompanyId).catch(() => []),
        apiService.getInvoices(actualCompanyId).catch(() => []),
        apiService.getInventory(actualCompanyId).catch(() => [])
      ]);

      const reportData = {
        type: reportType,
        currency: selectedCurrency,
        stats,
        transactions,
        invoices,
        inventory,
        period: getReportPeriod(reportType),
        generatedAt: new Date()
      };

      // Generate PDF
      const pdfContent = generateProfessionalPDF(reportData);
      downloadFile({
        name: `rapport-${reportType}-${new Date().toISOString().split('T')[0]}.pdf.html`,
        content: pdfContent,
        type: 'text/html'
      });

      // Generate CSV
      const csvContent = generateProfessionalCSV(reportData);
      downloadFile({
        name: `rapport-${reportType}-${new Date().toISOString().split('T')[0]}.csv`,
        content: csvContent,
        type: 'text/csv'
      });

      toast.success(`Rapport ${getReportTitle(reportType)} généré ! (PDF + CSV)`);
    } catch (error) {
      console.error('Report error:', error);
      toast.error('Erreur lors de la génération');
    } finally {
      setLoading(false);
    }
  };

  const downloadFile = ({ name, content, type }) => {
    const blob = new Blob([content], { type });
    const url = URL.createObjectURL(blob);
    const link = document.createElement('a');
    link.href = url;
    link.download = name;
    document.body.appendChild(link);
    link.click();
    document.body.removeChild(link);
    URL.revokeObjectURL(url);
  };

  const generateProfessionalPDF = (data) => {
    const { stats, transactions, type, currency, period } = data;
    const filteredTransactions = filterTransactionsByPeriod(transactions, type);
    const income = filteredTransactions.filter(t => t.type === 'income');
    const expenses = filteredTransactions.filter(t => t.type === 'expense');
    const totalIncome = income.reduce((sum, t) => sum + (t.amount || 0), 0);
    const totalExpenses = expenses.reduce((sum, t) => sum + (t.amount || 0), 0);
    const netProfit = totalIncome - totalExpenses;

    return `<!DOCTYPE html>
<html lang="fr">
<head>
    <meta charset="utf-8">
    <title>Rapport ${getReportTitle(type)} - Happy Deal Transit</title>
    <style>
        @page { margin: 15mm; size: A4 portrait; }
        body { font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif; line-height: 1.6; color: #333; }
        .header { background: linear-gradient(135deg, #1e40af 0%, #3b82f6 100%); color: white; padding: 20px 30px; margin-bottom: 20px; border-radius: 10px; display: flex; justify-content: space-between; align-items: center; }
        .logo { font-size: 24px; font-weight: bold; }
        .logo .orange { color: #f97316; }
        .company-info { text-align: right; font-size: 12px; opacity: 0.9; }
        .report-title { background: #f8fafc; padding: 20px; text-align: center; margin-bottom: 20px; border-radius: 10px; border: 2px solid #e2e8f0; }
        .report-title h1 { font-size: 24px; color: #1e40af; margin-bottom: 5px; }
        .report-period { color: #64748b; font-size: 12px; }
        .summary-grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(200px, 1fr)); gap: 15px; margin-bottom: 20px; }
        .summary-card { background: white; padding: 15px; border-radius: 8px; box-shadow: 0 2px 6px rgba(0,0,0,0.05); border-left: 4px solid #3b82f6; text-align: center; }
        .summary-card.income { border-left-color: #10b981; }
        .summary-card.expense { border-left-color: #ef4444; }
        .summary-card.profit { border-left-color: #8b5cf6; }
        .summary-value { font-size: 20px; font-weight: bold; margin-bottom: 5px; }
        .income .summary-value { color: #10b981; }
        .expense .summary-value { color: #ef4444; }
        .profit .summary-value { color: #8b5cf6; }
        .summary-label { font-size: 12px; color: #64748b; text-transform: uppercase; letter-spacing: 1px; }
        .footer { margin-top: 30px; padding: 15px; background: #f8fafc; border-radius: 8px; text-align: center; font-size: 10px; color: #64748b; border-top: 1px solid #e2e8f0; }
    </style>
</head>
<body>
    <div class="header">
        <div class="logo"><span class="orange">HD</span>Transit</div>
        <div class="company-info">
            <strong>Happy Deal Transit</strong><br>
            9, Plateaux ESSALAM, Casablanca<br>
            Tél: +212 5 22 20 85 94<br>
            Email: contact@hdtransit.com
        </div>
    </div>
    <div class="report-title">
        <h1>${getReportTitle(type)}</h1>
        <div class="report-period">
            Période: ${period}<br>
            Généré le: ${data.generatedAt.toLocaleDateString('fr-FR')} à ${data.generatedAt.toLocaleTimeString('fr-FR')}<br>
            Devise: ${currency}
        </div>
    </div>
    <div class="summary-grid">
        <div class="summary-card income">
            <div class="summary-value">${formatCurrency(totalIncome, currency)}</div>
            <div class="summary-label">Revenus Totaux</div>
        </div>
        <div class="summary-card expense">
            <div class="summary-value">${formatCurrency(totalExpenses, currency)}</div>
            <div class="summary-label">Dépenses Totales</div>
        </div>
        <div class="summary-card profit">
            <div class="summary-value">${formatCurrency(netProfit, currency)}</div>
            <div class="summary-label">Bénéfice Net</div>
        </div>
    </div>
    <div class="footer">
        <p><strong>Happy Deal Transit - Système ERP Professionnel</strong></p>
        <p>Rapport généré automatiquement le ${data.generatedAt.toLocaleString('fr-FR')}</p>
        <p>Pour toute question: contact@hdtransit.com</p>
    </div>
</body>
</html>`;
  };

  const generateProfessionalCSV = (data) => {
    const { stats, transactions, invoices, inventory, type, currency, period } = data;
    let csv = `HAPPY DEAL TRANSIT - RAPPORT ${getReportTitle(type).toUpperCase()}\n`;
    csv += `Période,${period}\n`;
    csv += `Date de génération,${data.generatedAt.toLocaleString('fr-FR')}\n`;
    csv += `Devise,${currency}\n\n`;
    csv += `RÉSUMÉ FINANCIER\n`;
    csv += `Revenus Total,${stats?.total_income || 0}\n`;
    csv += `Dépenses Total,${stats?.total_expenses || 0}\n`;
    csv += `Bénéfice Net,${(stats?.total_income || 0) - (stats?.total_expenses || 0)}\n`;
    return csv;
  };

  const filterTransactionsByPeriod = (transactions, reportType) => {
    if (!transactions?.length) return [];
    const now = new Date();
    let startDate;
    switch (reportType) {
      case 'weekly': startDate = new Date(now.getTime() - 7 * 24 * 60 * 60 * 1000); break;
      case 'monthly': startDate = new Date(now.getFullYear(), now.getMonth(), 1); break;
      case '6months': startDate = new Date(now.getTime() - 6 * 30 * 24 * 60 * 60 * 1000); break;
      case 'yearly': startDate = new Date(now.getFullYear(), 0, 1); break;
      default: return transactions;
    }
    return transactions.filter(t => new Date(t.date) >= startDate);
  };

  const getReportTitle = (reportType) => ({
    'weekly': 'Rapport Hebdomadaire',
    'monthly': 'Rapport Mensuel',
    '6months': 'Rapport Semestriel',
    'yearly': 'Rapport Annuel'
  }[reportType] || 'Rapport Personnalisé');

  const getReportPeriod = (reportType) => {
    const now = new Date();
    switch (reportType) {
      case 'weekly':
        return `${new Date(now.getTime() - 7 * 24 * 60 * 60 * 1000).toLocaleDateString('fr-FR')} - ${now.toLocaleDateString('fr-FR')}`;
      case 'monthly':
        return `${new Date(now.getFullYear(), now.getMonth(), 1).toLocaleDateString('fr-FR')} - ${now.toLocaleDateString('fr-FR')}`;
      case '6months':
        return `${new Date(now.getTime() - 6 * 30 * 24 * 60 * 60 * 1000).toLocaleDateString('fr-FR')} - ${now.toLocaleDateString('fr-FR')}`;
      case 'yearly':
        return `${new Date(now.getFullYear(), 0, 1).toLocaleDateString('fr-FR')} - ${now.toLocaleDateString('fr-FR')}`;
      default:
        return 'Période complète';
    }
  };

  const formatCurrency = (amount, currency = 'MAD') => {
    const currencies = {
      'MAD': { symbol: 'DH', format: 'fr-MA' },
      'EUR': { symbol: '€', format: 'fr-FR' },
      'USD': { symbol: '$', format: 'en-US' },
      'GBP': { symbol: '£', format: 'en-GB' }
    };
    const info = currencies[currency] || currencies['MAD'];
    return `${(amount || 0).toLocaleString(info.format, { minimumFractionDigits: 2, maximumFractionDigits: 2 })} ${info.symbol}`;
  };

  const reportTypes = [
    { type: 'weekly', title: 'Rapport Hebdomadaire', description: '7 derniers jours', icon: Calendar, color: 'bg-blue-600' },
    { type: 'monthly', title: 'Rapport Mensuel', description: 'Mois en cours', icon: FileText, color: 'bg-green-600' },
    { type: '6months', title: 'Rapport Semestriel', description: '6 derniers mois', icon: TrendingUp, color: 'bg-purple-600' },
    { type: 'yearly', title: 'Rapport Annuel', description: 'Année complète', icon: FileText, color: 'bg-red-600' }
  ];

  return (
    <div className="space-y-6 max-w-7xl mx-auto">
      <div className="flex flex-col lg:flex-row justify-between items-start lg:items-center gap-4">
        <div>
          <h1 className="text-2xl font-bold text-gray-900">Rapports & Analytics</h1>
          <p className="text-gray-600">Génération professionnelle multi-format</p>
        </div>
        <select
          value={selectedCurrency}
          onChange={(e) => setSelectedCurrency(e.target.value)}
          className="border rounded px-3 py-2"
        >
          <option value="MAD">MAD (Dirham)</option>
          <option value="USD">USD ($)</option>
          <option value="EUR">EUR (€)</option>
          <option value="GBP">GBP (£)</option>
        </select>
      </div>

      <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-6">
        {reportTypes.map((report) => (
          <div key={report.type} className="bg-white rounded-xl shadow-sm border border-gray-200 overflow-hidden">
            <div className={`${report.color} px-6 py-4`}>
              <div className="flex items-center justify-between text-white">
                <report.icon className="h-8 w-8" />
                <Folder className="h-6 w-6 opacity-70" />
              </div>
            </div>
            <div className="px-6 py-4">
              <h3 className="text-lg font-semibold text-gray-900 mb-2">{report.title}</h3>
              <p className="text-sm text-gray-600 mb-4">{report.description}</p>
              <button
                onClick={() => generateComprehensiveReport(report.type)}
                disabled={loading}
                className="w-full bg-gray-100 hover:bg-gray-200 disabled:bg-gray-50 text-gray-800 py-2 px-4 rounded-lg flex items-center justify-center gap-2 transition-colors"
              >
                {loading ? (
                  <div className="animate-spin rounded-full h-4 w-4 border-b-2 border-gray-600"></div>
                ) : (
                  <Download className="h-4 w-4" />
                )}
                {loading ? 'Génération...' : 'Générer Rapport'}
              </button>
            </div>
          </div>
        ))}
      </div>
    </div>
  );
};

export default Reports;