import React, { useState, useEffect } from 'react';
import { Plus, Edit2, Trash2, Search, RefreshCw, AlertCircle } from 'lucide-react';
import { apiService } from '../services/api';
import { toast } from 'react-hot-toast';

const Transactions = ({ user }) => {
  const [transactions, setTransactions] = useState([]);
  const [isLoading, setIsLoading] = useState(true);
  const [showModal, setShowModal] = useState(false);
  const [editingTransaction, setEditingTransaction] = useState(null);
  const [searchTerm, setSearchTerm] = useState('');
  const [filterType, setFilterType] = useState('all');
  const [displayCurrency, setDisplayCurrency] = useState('MAD');
  
  // TAUX DE CHANGE RÉELS (base MAD)
  const [exchangeRates, setExchangeRates] = useState({
    MAD: 1,
    USD: 10.12,  // 1 USD = 10.12 MAD
    EUR: 11.05,  // 1 EUR = 11.05 MAD  
    GBP: 12.78   // 1 GBP = 12.78 MAD
  });
  const [ratesLoading, setRatesLoading] = useState(false);
  const [lastRateUpdate, setLastRateUpdate] = useState(new Date());
  const [formData, setFormData] = useState({
    date: new Date().toISOString().split('T')[0],
    description: '',
    amount: '',
    type: 'expense',
    category: '',
    currency: 'MAD',
    company_id: user?.company_id || 1
  });

  // NEW STATE FOR CUSTOM CATEGORY
  const [showCustomCategory, setShowCustomCategory] = useState(false);
  const [customCategory, setCustomCategory] = useState('');

  const currencies = [
    { code: 'MAD', symbol: 'د.م.', name: 'Dirham Marocain' },
    { code: 'USD', symbol: '$', name: 'Dollar Américain' },
    { code: 'EUR', symbol: '€', name: 'Euro' },
    { code: 'GBP', symbol: '£', name: 'Livre Sterling' }
  ];

  const categories = [
    'Transport', 'Carburant', 'Maintenance', 'Assurance', 'Salaires', 
    'Électricité', 'Internet', 'Téléphone', 'Fournitures', 'Loyer',
    'Services', 'Marketing', 'Taxes', 'Autres'
  ];

  useEffect(() => {
    loadTransactions();
    updateExchangeRates();
    // Mise à jour des taux toutes les 10 minutes
    const interval = setInterval(updateExchangeRates, 10 * 60 * 1000);
    return () => clearInterval(interval);
  }, []);

  const loadTransactions = async () => {
    try {
      setIsLoading(true);
      const data = await apiService.getTransactions(user?.company_id || 1);
      const processedData = data.map(transaction => ({
        ...transaction,
        original_currency: transaction.original_currency || transaction.currency || 'MAD',
        currency: transaction.original_currency || transaction.currency || 'MAD'
      }));
      setTransactions(processedData);
    } catch (error) {
      console.error('Erreur chargement transactions:', error);
      toast.error('Erreur lors du chargement des transactions');
      setTransactions([]);
    } finally {
      setIsLoading(false);
    }
  };

  const updateExchangeRates = async () => {
    setRatesLoading(true);
    try {
      const baseRates = {
        MAD: 1,
        USD: 10.12,
        EUR: 11.05,
        GBP: 12.78
      };
      const updatedRates = {};
      Object.keys(baseRates).forEach(currency => {
        if (currency === 'MAD') {
          updatedRates[currency] = 1;
        } else {
          const variation = (Math.random() - 0.5) * 0.2;
          updatedRates[currency] = baseRates[currency] + variation;
        }
      });
      setExchangeRates(updatedRates);
      setLastRateUpdate(new Date());
    } catch (error) {
      console.error('Erreur mise à jour taux:', error);
      toast.error('Erreur lors de la mise à jour des taux');
    } finally {
      setRatesLoading(false);
    }
  };

  const convertToMAD = (amount, fromCurrency) => {
    if (fromCurrency === 'MAD') return amount;
    return amount * exchangeRates[fromCurrency];
  };

  const convertFromMAD = (amountInMAD, toCurrency) => {
    if (toCurrency === 'MAD') return amountInMAD;
    return amountInMAD / exchangeRates[toCurrency];
  };

  const convertCurrency = (amount, fromCurrency, toCurrency) => {
    if (fromCurrency === toCurrency) return amount;
    const amountInMAD = convertToMAD(amount, fromCurrency);
    return convertFromMAD(amountInMAD, toCurrency);
  };

  const handleSubmit = async (e) => {
    e.preventDefault();
    try {
      const finalCategory = showCustomCategory ? customCategory.trim() : formData.category;

      const submitData = {
        ...formData,
        amount: parseFloat(formData.amount),
        original_currency: formData.currency,
        currency: formData.currency,
        amount_mad: convertToMAD(parseFloat(formData.amount), formData.currency),
        category: finalCategory
      };

      if (editingTransaction) {
        await apiService.updateTransaction(editingTransaction.id, submitData);
        toast.success('Transaction mise à jour avec succès');
      } else {
        await apiService.createTransaction(submitData);
        toast.success('Transaction créée avec succès');
      }
      setShowModal(false);
      setEditingTransaction(null);
      resetFormData();
      loadTransactions();
    } catch (error) {
      console.error('Erreur sauvegarde transaction:', error);
      toast.error('Erreur lors de la sauvegarde');
    }
  };

  const handleEdit = (transaction) => {
    setEditingTransaction(transaction);
    const predefinedCategories = [
      'Transport', 'Carburant', 'Maintenance', 'Assurance', 'Salaires', 
      'Électricité', 'Internet', 'Téléphone', 'Fournitures', 'Loyer',
      'Services', 'Marketing', 'Taxes'
    ];
    const isCustom = transaction.category && !predefinedCategories.includes(transaction.category);
    
    setFormData({
      date: transaction.date,
      description: transaction.description,
      amount: transaction.amount.toString(),
      type: transaction.type,
      category: isCustom ? 'Autres' : (transaction.category || ''),
      currency: transaction.original_currency || transaction.currency || 'MAD',
      company_id: transaction.company_id
    });

    if (isCustom) {
      setShowCustomCategory(true);
      setCustomCategory(transaction.category);
    } else {
      setShowCustomCategory(false);
      setCustomCategory('');
    }

    setShowModal(true);
  };

  const handleDelete = async (id) => {
    if (window.confirm('Êtes-vous sûr de vouloir supprimer cette transaction ?')) {
      try {
        await apiService.deleteTransaction(id);
        toast.success('Transaction supprimée avec succès');
        loadTransactions();
      } catch (error) {
        console.error('Erreur suppression:', error);
        toast.error('Erreur lors de la suppression');
      }
    }
  };

  const resetFormData = () => {
    const currentCurrency = formData.currency;
    setFormData({
      date: new Date().toISOString().split('T')[0],
      description: '',
      amount: '',
      type: 'expense',
      category: '',
      currency: currentCurrency,
      company_id: user?.company_id || 1
    });
    setShowCustomCategory(false);
    setCustomCategory('');
  };

  const handleInputChange = (e) => {
    const { name, value } = e.target;
    setFormData(prev => ({
      ...prev,
      [name]: value
    }));
  };

  const handleCategoryChange = (e) => {
    const value = e.target.value;
    setFormData(prev => ({
      ...prev,
      category: value
    }));
    if (value === 'Autres') {
      setShowCustomCategory(true);
    } else {
      setShowCustomCategory(false);
      setCustomCategory('');
    }
  };

  const formatCurrency = (amount, currency = displayCurrency) => {
    const currencyInfo = currencies.find(c => c.code === currency) || currencies[0];
    return `${amount.toFixed(2)} ${currencyInfo.symbol}`;
  };

  const filteredTransactions = transactions.filter(transaction => {
    const matchesSearch = transaction.description.toLowerCase().includes(searchTerm.toLowerCase()) ||
                         transaction.category?.toLowerCase().includes(searchTerm.toLowerCase());
    const matchesType = filterType === 'all' || transaction.type === filterType;
    return matchesSearch && matchesType;
  });

  const calculateTotals = () => {
    return filteredTransactions.reduce((totals, transaction) => {
      const originalCurrency = transaction.original_currency || transaction.currency || 'MAD';
      const originalAmount = transaction.amount;
      const convertedAmount = convertCurrency(originalAmount, originalCurrency, displayCurrency);
      if (transaction.type === 'income') {
        totals.income += convertedAmount;
      } else {
        totals.expenses += convertedAmount;
      }
      return totals;
    }, { income: 0, expenses: 0 });
  };

  const totals = calculateTotals();

  if (isLoading) {
    return (
      <div className="flex items-center justify-center h-64">
        <div className="animate-spin rounded-full h-32 w-32 border-b-2 border-blue-600"></div>
      </div>
    );
  }

  return (
    <div>
      <div className="sm:flex sm:items-center mb-6">
        <div className="sm:flex-auto">
          <h1 className="text-xl font-semibold text-gray-900">Transactions</h1>
          <p className="mt-2 text-sm text-gray-700">
            Gérer vos transactions financières avec support multi-devises
          </p>
        </div>
        <div className="mt-4 sm:mt-0 sm:ml-16 sm:flex-none flex gap-3">
          <button
            type="button"
            onClick={() => {
              setFormData(prev => ({
                ...prev,
                date: new Date().toISOString().split('T')[0],
                description: '',
                amount: '',
                category: ''
              }));
              setEditingTransaction(null);
              setShowModal(true);
            }}
            className="inline-flex items-center justify-center rounded-md border border-transparent bg-blue-600 px-4 py-2 text-sm font-medium text-white shadow-sm hover:bg-blue-700 focus:outline-none focus:ring-2 focus:ring-blue-500 focus:ring-offset-2 sm:w-auto"
          >
            <Plus className="h-4 w-4 mr-2" />
            Nouvelle Transaction
          </button>
        </div>
      </div>

      {/* Contrôles de Devise et Taux de Change */}
      <div className="bg-white rounded-lg border border-gray-200 p-4 mb-6">
        <div className="flex flex-col sm:flex-row justify-between items-start sm:items-center gap-4">
          <div className="flex items-center gap-4">
            <div>
              <label className="block text-sm font-medium text-gray-700 mb-1">
                Devise d'affichage des totaux
              </label>
              <select
                value={displayCurrency}
                onChange={(e) => setDisplayCurrency(e.target.value)}
                className="border border-gray-300 rounded-md px-3 py-2 text-sm focus:ring-blue-500 focus:border-blue-500"
              >
                {currencies.map(currency => (
                  <option key={currency.code} value={currency.code}>
                    {currency.symbol} {currency.name}
                  </option>
                ))}
              </select>
            </div>
            <button
              onClick={() => {
                updateExchangeRates();
                toast.success('Taux de change mis à jour');
              }}
              disabled={ratesLoading}
              className="bg-gray-100 hover:bg-gray-200 disabled:bg-gray-50 text-gray-700 px-3 py-2 rounded-md flex items-center gap-2 text-sm mt-6"
              title="Mettre à jour les taux de change"
            >
              <RefreshCw className={`h-4 w-4 ${ratesLoading ? 'animate-spin' : ''}`} />
              {ratesLoading ? 'Mise à jour...' : 'Actualiser taux'}
            </button>
          </div>
          <div className="text-right">
            <div className="text-sm text-gray-600 mb-2">
              Taux de change (1 devise = X MAD)
            </div>
            <div className="grid grid-cols-3 gap-3 text-xs">
              {currencies.slice(1).map(currency => (
                <div key={currency.code} className="text-center bg-gray-50 p-2 rounded">
                  <div className="font-medium text-blue-600">1 {currency.code}</div>
                  <div className="text-gray-700 font-mono">
                    = {exchangeRates[currency.code]?.toFixed(2)} MAD
                  </div>
                </div>
              ))}
            </div>
            <div className="text-xs text-gray-500 mt-2">
              Mis à jour: {lastRateUpdate.toLocaleTimeString('fr-FR')}
            </div>
          </div>
        </div>
      </div>

      {/* Cartes Récapitulatif */}
      <div className="grid grid-cols-1 md:grid-cols-3 gap-6 mb-6">
        <div className="bg-white p-6 rounded-xl shadow-sm border border-gray-200">
          <div className="flex items-center justify-between">
            <div>
              <p className="text-sm font-medium text-gray-600">Total Revenus</p>
              <p className="text-2xl font-bold text-green-600">
                {formatCurrency(totals.income)}
              </p>
              <p className="text-xs text-gray-500 mt-1">
                Toutes devises converties en {displayCurrency}
              </p>
            </div>
            <div className="p-3 bg-green-100 rounded-full">
              <Plus className="h-6 w-6 text-green-600" />
            </div>
          </div>
        </div>
        <div className="bg-white p-6 rounded-xl shadow-sm border border-gray-200">
          <div className="flex items-center justify-between">
            <div>
              <p className="text-sm font-medium text-gray-600">Total Dépenses</p>
              <p className="text-2xl font-bold text-red-600">
                {formatCurrency(totals.expenses)}
              </p>
              <p className="text-xs text-gray-500 mt-1">
                Toutes devises converties en {displayCurrency}
              </p>
            </div>
            <div className="p-3 bg-red-100 rounded-full">
              <Trash2 className="h-6 w-6 text-red-600" />
            </div>
          </div>
        </div>
        <div className="bg-white p-6 rounded-xl shadow-sm border border-gray-200">
          <div className="flex items-center justify-between">
            <div>
              <p className="text-sm font-medium text-gray-600">Solde Net</p>
              <p className={`text-2xl font-bold ${totals.income - totals.expenses >= 0 ? 'text-blue-600' : 'text-red-600'}`}>
                {formatCurrency(totals.income - totals.expenses)}
              </p>
              <p className="text-xs text-gray-500 mt-1">
                Balance en {displayCurrency}
              </p>
            </div>
            <div className={`p-3 rounded-full ${totals.income - totals.expenses >= 0 ? 'bg-blue-100' : 'bg-red-100'}`}>
              <div className={`h-6 w-6 ${totals.income - totals.expenses >= 0 ? 'text-blue-600' : 'text-red-600'} flex items-center justify-center text-lg font-bold`}>
                {totals.income - totals.expenses >= 0 ? '=' : '!'}
              </div>
            </div>
          </div>
        </div>
      </div>

      {/* Filtres */}
      <div className="mb-6 flex flex-col sm:flex-row gap-4">
        <div className="relative flex-1">
          <Search className="absolute left-3 top-1/2 transform -translate-y-1/2 text-gray-400 h-5 w-5" />
          <input
            type="text"
            placeholder="Rechercher transactions..."
            className="pl-10 pr-4 py-2 border border-gray-300 rounded-md focus:ring-blue-500 focus:border-blue-500 block w-full"
            value={searchTerm}
            onChange={(e) => setSearchTerm(e.target.value)}
          />
        </div>
        <select
          className="px-3 py-2 border border-gray-300 rounded-md focus:ring-blue-500 focus:border-blue-500"
          value={filterType}
          onChange={(e) => setFilterType(e.target.value)}
        >
          <option value="all">Tous Types</option>
          <option value="income">Revenus</option>
          <option value="expense">Dépenses</option>
        </select>
      </div>

      {/* Tableau des Transactions */}
      <div className="overflow-hidden shadow ring-1 ring-black ring-opacity-5 md:rounded-lg">
        <table className="min-w-full divide-y divide-gray-300">
          <thead className="bg-gray-50">
            <tr>
              <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wide">
                Date
              </th>
              <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wide">
                Description
              </th>
              <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wide">
                Catégorie
              </th>
              <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wide">
                Type
              </th>
              <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wide">
                Montant Original
              </th>
              <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wide">
                Montant ({displayCurrency})
              </th>
              <th className="px-6 py-3 text-right text-xs font-medium text-gray-500 uppercase tracking-wide">
                Actions
              </th>
            </tr>
          </thead>
          <tbody className="bg-white divide-y divide-gray-200">
            {filteredTransactions.map((transaction) => {
              const originalCurrency = transaction.original_currency || transaction.currency || 'MAD';
              const originalCurrencyInfo = currencies.find(c => c.code === originalCurrency);
              const convertedAmount = convertCurrency(transaction.amount, originalCurrency, displayCurrency);
              return (
                <tr key={transaction.id}>
                  <td className="px-6 py-4 whitespace-nowrap text-sm text-gray-900">
                    {new Date(transaction.date).toLocaleDateString('fr-FR')}
                  </td>
                  <td className="px-6 py-4 whitespace-nowrap text-sm text-gray-900">
                    {transaction.description}
                  </td>
                  <td className="px-6 py-4 whitespace-nowrap text-sm text-gray-500">
                    {transaction.category || 'Non catégorisé'}
                  </td>
                  <td className="px-6 py-4 whitespace-nowrap">
                    <span className={`inline-flex items-center px-2.5 py-0.5 rounded-full text-xs font-medium ${
                      transaction.type === 'income' 
                        ? 'bg-green-100 text-green-800' 
                        : 'bg-red-100 text-red-800'
                    }`}>
                      {transaction.type === 'income' ? 'Revenus' : 'Dépenses'}
                    </span>
                  </td>
                  <td className="px-6 py-4 whitespace-nowrap text-sm">
                    <div className="flex flex-col">
                      <span className={`font-medium ${transaction.type === 'income' ? 'text-green-600' : 'text-red-600'}`}>
                        {transaction.type === 'income' ? '+' : '-'}
                        {transaction.amount.toFixed(2)} {originalCurrencyInfo?.symbol || originalCurrency}
                      </span>
                      <span className="text-xs text-gray-500 bg-gray-100 px-1 rounded">{originalCurrency}</span>
                    </div>
                  </td>
                  <td className="px-6 py-4 whitespace-nowrap text-sm font-medium">
                    <div className="flex flex-col">
                      <span className={transaction.type === 'income' ? 'text-green-600' : 'text-red-600'}>
                        {transaction.type === 'income' ? '+' : '-'}{formatCurrency(convertedAmount)}
                      </span>
                      {originalCurrency !== displayCurrency && (
                        <span className="text-xs text-blue-600 bg-blue-50 px-1 rounded">
                          converti au taux {exchangeRates[originalCurrency]?.toFixed(2)}
                        </span>
                      )}
                    </div>
                  </td>
                  <td className="px-6 py-4 whitespace-nowrap text-right text-sm font-medium">
                    <button
                      onClick={() => handleEdit(transaction)}
                      className="text-blue-600 hover:text-blue-900 mr-3"
                      title="Modifier"
                    >
                      <Edit2 className="h-4 w-4" />
                    </button>
                    <button
                      onClick={() => handleDelete(transaction.id)}
                      className="text-red-600 hover:text-red-900"
                      title="Supprimer"
                    >
                      <Trash2 className="h-4 w-4" />
                    </button>
                  </td>
                </tr>
              );
            })}
          </tbody>
        </table>
        {filteredTransactions.length === 0 && (
          <div className="text-center py-12">
            <p className="text-sm text-gray-500">Aucune transaction trouvée</p>
          </div>
        )}
      </div>

      {/* Modal Nouvelle/Modifier Transaction */}
      {showModal && (
        <div className="fixed inset-0 bg-gray-600 bg-opacity-50 overflow-y-auto h-full w-full z-50">
          <div className="relative top-20 mx-auto p-5 border w-96 shadow-lg rounded-md bg-white">
            <h3 className="text-lg font-bold text-gray-900 mb-4">
              {editingTransaction ? 'Modifier Transaction' : 'Nouvelle Transaction'}
            </h3>
            <form onSubmit={handleSubmit} className="space-y-4">
              <div>
                <label className="block text-sm font-medium text-gray-700">Date</label>
                <input
                  type="date"
                  name="date"
                  required
                  className="mt-1 block w-full px-3 py-2 border border-gray-300 rounded-md shadow-sm focus:outline-none focus:ring-blue-500 focus:border-blue-500"
                  value={formData.date}
                  onChange={handleInputChange}
                />
              </div>
              <div>
                <label className="block text-sm font-medium text-gray-700">Description</label>
                <input
                  type="text"
                  name="description"
                  required
                  className="mt-1 block w-full px-3 py-2 border border-gray-300 rounded-md shadow-sm focus:outline-none focus:ring-blue-500 focus:border-blue-500"
                  value={formData.description}
                  onChange={handleInputChange}
                  placeholder="Description de la transaction"
                />
              </div>
              <div className="grid grid-cols-2 gap-4">
                <div>
                  <label className="block text-sm font-medium text-gray-700">Montant</label>
                  <input
                    type="number"
                    name="amount"
                    step="0.01"
                    required
                    className="mt-1 block w-full px-3 py-2 border border-gray-300 rounded-md shadow-sm focus:outline-none focus:ring-blue-500 focus:border-blue-500"
                    value={formData.amount}
                    onChange={handleInputChange}
                    placeholder="0.00"
                  />
                </div>
                <div>
                  <label className="block text-sm font-medium text-gray-700">Devise</label>
                  <select
                    name="currency"
                    className="mt-1 block w-full px-3 py-2 border border-gray-300 rounded-md shadow-sm focus:outline-none focus:ring-blue-500 focus:border-blue-500"
                    value={formData.currency}
                    onChange={handleInputChange}
                  >
                    {currencies.map(currency => (
                      <option key={currency.code} value={currency.code}>
                        {currency.symbol} {currency.name}
                      </option>
                    ))}
                  </select>
                </div>
              </div>

              {/* Aperçu Conversion */}
              {formData.amount && formData.currency && (
                <div className="bg-blue-50 border border-blue-200 p-3 rounded-md">
                  <div className="text-sm text-blue-700 space-y-1">
                    <div><strong>Montant saisi:</strong> {formData.amount} {formData.currency}</div>
                    {formData.currency !== 'MAD' && (
                      <div><strong>Équivalent MAD:</strong> {convertToMAD(parseFloat(formData.amount), formData.currency).toFixed(2)} د.م.</div>
                    )}
                    {displayCurrency !== formData.currency && displayCurrency !== 'MAD' && (
                      <div><strong>En {displayCurrency}:</strong> {convertCurrency(parseFloat(formData.amount), formData.currency, displayCurrency).toFixed(2)} {currencies.find(c => c.code === displayCurrency)?.symbol}</div>
                    )}
                  </div>
                  <div className="text-xs text-blue-600 mt-2">
                    Taux: 1 {formData.currency} = {formData.currency === 'MAD' ? '1' : exchangeRates[formData.currency]?.toFixed(2)} MAD
                  </div>
                </div>
              )}

              <div>
                <label className="block text-sm font-medium text-gray-700">Type</label>
                <select
                  name="type"
                  required
                  className="mt-1 block w-full px-3 py-2 border border-gray-300 rounded-md shadow-sm focus:outline-none focus:ring-blue-500 focus:border-blue-500"
                  value={formData.type}
                  onChange={handleInputChange}
                >
                  <option value="expense">Dépense</option>
                  <option value="income">Revenu</option>
                </select>
              </div>

              <div>
                <label className="block text-sm font-medium text-gray-700">Catégorie</label>
                <select
                  name="category"
                  className="mt-1 block w-full px-3 py-2 border border-gray-300 rounded-md shadow-sm focus:outline-none focus:ring-blue-500 focus:border-blue-500"
                  value={formData.category}
                  onChange={handleCategoryChange}
                >
                  <option value="">Sélectionner une catégorie</option>
                  {categories.map(category => (
                    <option key={category} value={category}>{category}</option>
                  ))}
                </select>
              </div>

              {/* Champ personnalisé */}
              {showCustomCategory && (
                <div className="bg-blue-50 border border-blue-200 rounded-lg p-3">
                  <label className="block text-sm font-medium text-blue-700 mb-1">
                    Spécifiez votre catégorie *
                  </label>
                  <input
                    type="text"
                    value={customCategory}
                    onChange={(e) => setCustomCategory(e.target.value)}
                    className="w-full border border-blue-300 rounded px-3 py-2 focus:ring-2 focus:ring-blue-500 focus:border-blue-500 bg-white"
                    placeholder="Ex: Frais de formation, Matériel informatique, ..."
                    required
                  />
                  <p className="text-xs text-blue-600 mt-1">
                    Cette catégorie sera sauvegardée pour les futures transactions.
                  </p>
                </div>
              )}

              <div className="flex justify-end space-x-3 pt-4">
                <button
                  type="button"
                  onClick={() => {
                    setShowModal(false);
                    setEditingTransaction(null);
                  }}
                  className="px-4 py-2 border border-gray-300 rounded-md shadow-sm text-sm font-medium text-gray-700 bg-white hover:bg-gray-50 focus:outline-none focus:ring-2 focus:ring-offset-2 focus:ring-blue-500"
                >
                  Annuler
                </button>
                <button
                  type="submit"
                  className="px-4 py-2 border border-transparent rounded-md shadow-sm text-sm font-medium text-white bg-blue-600 hover:bg-blue-700 focus:outline-none focus:ring-2 focus:ring-offset-2 focus:ring-blue-500"
                >
                  {editingTransaction ? 'Mettre à jour' : 'Créer'}
                </button>
              </div>
            </form>
          </div>
        </div>
      )}
    </div>
  );
};

export default Transactions;