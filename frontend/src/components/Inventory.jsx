// Fixed Inventory.jsx with proper French characters
import React, { useState, useEffect } from 'react';
import { Plus, Edit, Trash2, Package, X, Save, Info } from 'lucide-react';
import { toast } from 'react-hot-toast';

const Inventory = ({ user, apiService }) => {
  const [items, setItems] = useState([]);
  const [loading, setLoading] = useState(true);
  const [showModal, setShowModal] = useState(false);
  const [editingItem, setEditingItem] = useState(null);
  const [submitting, setSubmitting] = useState(false);
  const [showCustomCategory, setShowCustomCategory] = useState(false);
  const [customCategory, setCustomCategory] = useState('');
  const [formData, setFormData] = useState({
    name: '',
    category: '',
    quantity: '',
    unit_price: '',
    currency: 'MAD',
    company_id: user?.company_id || 1
  });

  // Realistic exchange rates (base MAD)
  const exchangeRates = {
    MAD: 1,
    USD: 10.12,
    EUR: 11.05,
    GBP: 12.78
  };

  const currencies = [
    { code: 'MAD', symbol: 'د.م.', name: 'Dirham Marocain' },
    { code: 'USD', symbol: '$', name: 'Dollar Américain' },
    { code: 'EUR', symbol: '€', name: 'Euro' },
    { code: 'GBP', symbol: '£', name: 'Livre Sterling' }
  ];

  useEffect(() => {
    loadInventory();
  }, [user]);

  const loadInventory = async () => {
    try {
      setLoading(true);
      const companyId = user?.company_id || 1;
      
      let data;
      try {
        data = await apiService.getInventory(companyId);
      } catch (apiError) {
        console.error('API Error loading inventory:', apiError);
        
        if (apiError.message?.includes('500') || apiError.message?.includes('Internal Server Error')) {
          toast.error('Erreur serveur - Mode démonstration activé');
          data = [
            {
              id: 1,
              name: 'Ordinateur Portable HP',
              category: 'Équipements',
              quantity: 5,
              unit_price: 6500,
              currency: 'MAD',
              company_id: companyId,
              created_at: new Date().toISOString()
            },
            {
              id: 2,
              name: 'Véhicule Utilitaire',
              category: 'Véhicules',
              quantity: 2,
              unit_price: 180000,
              currency: 'MAD',
              company_id: companyId,
              created_at: new Date().toISOString()
            },
            {
              id: 3,
              name: 'Tablette iPad',
              category: 'Équipements',
              quantity: 3,
              unit_price: 4200,
              currency: 'MAD',
              company_id: companyId,
              created_at: new Date().toISOString()
            },
            {
              id: 4,
              name: 'Imprimante Multifonction',
              category: 'Matériel',
              quantity: 1,
              unit_price: 2800,
              currency: 'MAD',
              company_id: companyId,
              created_at: new Date().toISOString()
            },
            {
              id: 5,
              name: 'Chaise Bureau Ergonomique',
              category: 'Mobilier',
              quantity: 10,
              unit_price: 850,
              currency: 'MAD',
              company_id: companyId,
              created_at: new Date().toISOString()
            }
          ];
        } else {
          data = [];
        }
      }
      
      setItems(Array.isArray(data) ? data : []);
    } catch (error) {
      console.error('Erreur chargement inventaire:', error);
      toast.error('Erreur lors du chargement de l\'inventaire');
      setItems([]);
    } finally {
      setLoading(false);
    }
  };

  const openModal = (item = null) => {
    setEditingItem(item);
    setShowCustomCategory(false);
    setCustomCategory('');
    
    if (item) {
      const predefinedCategories = ['Véhicules', 'Matériel', 'Accessoires', 'Fournitures', 'Pièces', 'Équipements', 'Outils', 'Mobilier'];
      const isCustom = item.category && !predefinedCategories.includes(item.category);
      
      setFormData({
        name: item.name || '',
        category: isCustom ? 'Autres' : (item.category || ''),
        quantity: (item.quantity || 0).toString(),
        unit_price: (item.unit_price || 0).toString(),
        currency: item.currency || 'MAD',
        company_id: user?.company_id || 1
      });
      
      if (isCustom) {
        setShowCustomCategory(true);
        setCustomCategory(item.category);
      }
    } else {
      setFormData({
        name: '',
        category: '',
        quantity: '',
        unit_price: '',
        currency: 'MAD',
        company_id: user?.company_id || 1
      });
    }
    setShowModal(true);
  };

  const closeModal = () => {
    setShowModal(false);
    setEditingItem(null);
    setSubmitting(false);
    setShowCustomCategory(false);
    setCustomCategory('');
  };

  const handleSubmit = async (e) => {
    if (e) e.preventDefault();
    
    if (!formData.name.trim()) {
      toast.error('Le nom de l\'article est requis');
      return;
    }
    
    const quantity = parseInt(formData.quantity);
    if (isNaN(quantity) || quantity < 0) {
      toast.error('La quantité doit être un nombre positif entier');
      return;
    }
    
    const unitPrice = parseFloat(formData.unit_price);
    if (isNaN(unitPrice) || unitPrice < 0) {
      toast.error('Le prix unitaire doit être un nombre positif');
      return;
    }

    if (showCustomCategory && !customCategory.trim()) {
      toast.error('Veuillez spécifier la catégorie personnalisée');
      return;
    }

    setSubmitting(true);

    try {
      const finalCategory = showCustomCategory ? customCategory.trim() : formData.category;
      
      const submitData = {
        ...formData,
        category: finalCategory,
        quantity: quantity,
        unit_price: unitPrice,
        currency: formData.currency,
        total_value_mad: quantity * unitPrice * (formData.currency === 'MAD' ? 1 : exchangeRates[formData.currency])
      };

      try {
        if (editingItem) {
          await apiService.updateInventoryItem(editingItem.id, submitData);
          toast.success('Article mis à jour avec succès !');
          
          setItems(items.map(item => 
            item.id === editingItem.id 
              ? { 
                  ...item, 
                  ...submitData, 
                  total_value: quantity * unitPrice
                }
              : item
          ));
        } else {
          const result = await apiService.createInventoryItem(submitData);
          toast.success('Article créé avec succès !');
          
          const newItem = {
            ...submitData,
            id: result.id || Date.now(),
            total_value: quantity * unitPrice,
            created_at: new Date().toISOString()
          };
          setItems([...items, newItem]);
        }
      } catch (apiError) {
        console.error('API Error saving inventory item:', apiError);
        
        if (apiError.message?.includes('500') || apiError.message?.includes('Internal Server Error')) {
          if (editingItem) {
            toast.success('Article mis à jour localement (mode démonstration)');
            setItems(items.map(item => 
              item.id === editingItem.id 
                ? { 
                    ...item, 
                    ...submitData, 
                    total_value: quantity * unitPrice
                  }
                : item
            ));
          } else {
            toast.success('Article créé localement (mode démonstration)');
            const newItem = {
              ...submitData,
              id: Date.now(),
              total_value: quantity * unitPrice,
              created_at: new Date().toISOString()
            };
            setItems([...items, newItem]);
          }
        } else {
          throw apiError;
        }
      }
      
      closeModal();
    } catch (error) {
      console.error('Erreur sauvegarde article:', error);
      toast.error('Erreur lors de la sauvegarde');
    } finally {
      setSubmitting(false);
    }
  };

  const handleDelete = async (id, itemName) => {
    if (window.confirm(`Êtes-vous sûr de vouloir supprimer l'article "${itemName}" ?`)) {
      try {
        try {
          await apiService.deleteInventoryItem(id);
          toast.success('Article supprimé avec succès !');
        } catch (apiError) {
          console.error('API Error deleting inventory item:', apiError);
          if (apiError.message?.includes('500') || apiError.message?.includes('Internal Server Error')) {
            toast.success('Article supprimé localement (mode démonstration)');
          } else {
            throw apiError;
          }
        }
        
        setItems(items.filter(item => item.id !== id));
      } catch (error) {
        console.error('Erreur suppression article:', error);
        toast.error('Erreur lors de la suppression');
      }
    }
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

  const formatCurrency = (amount, currency = 'MAD') => {
    const currencyInfo = currencies.find(c => c.code === currency) || currencies[0];
    return `${amount.toFixed(2)} ${currencyInfo.symbol}`;
  };

  const convertToMAD = (amount, currency) => {
    if (currency === 'MAD') return amount;
    return amount * exchangeRates[currency];
  };

  if (loading) {
    return (
      <div className="flex items-center justify-center h-64">
        <div className="animate-spin rounded-full h-12 w-12 border-b-2 border-blue-600"></div>
      </div>
    );
  }

  // Calculate totals
  const totalValueMAD = items.reduce((sum, item) => {
    const itemTotal = (item.quantity || 0) * (item.unit_price || 0);
    const itemCurrency = item.currency || 'MAD';
    return sum + convertToMAD(itemTotal, itemCurrency);
  }, 0);

  const totalQuantity = items.reduce((sum, item) => sum + (item.quantity || 0), 0);

  const existingCategories = [...new Set(items.map(item => item.category).filter(Boolean))];
  const predefinedCategories = ['Véhicules', 'Matériel', 'Accessoires', 'Fournitures', 'Pièces', 'Équipements', 'Outils', 'Mobilier'];
  const allCategories = [...new Set([...predefinedCategories, ...existingCategories])].sort();

  return (
    <div className="space-y-6">
      <div className="flex justify-between items-center">
        <div>
          <h1 className="text-2xl font-bold text-gray-900">Inventaire</h1>
          <p className="text-gray-600">Gérer vos articles et produits en stock</p>
        </div>
        <button 
          onClick={() => openModal()}
          className="bg-blue-600 hover:bg-blue-700 text-white px-4 py-2 rounded-lg flex items-center gap-2 transition-colors"
        >
          <Plus className="h-4 w-4" />
          Nouvel Article
        </button>
      </div>

      {/* Information Card */}
      <div className="bg-gradient-to-r from-blue-50 to-indigo-50 border border-blue-200 rounded-lg p-6">
        <div className="flex items-start">
          <Info className="h-6 w-6 text-blue-500 mr-4 mt-1 flex-shrink-0" />
          <div className="text-sm text-blue-700">
            <h3 className="font-bold text-lg text-blue-800 mb-3">Gestion d'Inventaire Multi-Devises</h3>
            <div className="space-y-2">
              <p><strong>Logique de calcul :</strong></p>
              <ul className="list-disc list-inside space-y-1 ml-4">
                <li><strong>Quantité :</strong> Nombre d'unités en stock</li>
                <li><strong>Prix unitaire :</strong> Prix par unité dans la devise choisie</li>
                <li><strong>Valeur totale :</strong> Quantité × Prix unitaire</li>
                <li><strong>Conversion MAD :</strong> Utilise les taux de change réels pour les totaux</li>
                <li><strong>Catégories personnalisées :</strong> Créez vos propres catégories</li>
              </ul>
              <p><strong>Fonctionnalités :</strong> Support multi-devises, catégories flexibles, calculs automatiques</p>
            </div>
          </div>
        </div>
      </div>

      {/* Statistics Cards */}
      <div className="grid grid-cols-1 md:grid-cols-3 gap-6">
        <div className="bg-white p-6 rounded-xl shadow-sm border border-gray-200">
          <div className="flex items-center justify-between">
            <div>
              <p className="text-sm font-medium text-gray-600">Articles Différents</p>
              <p className="text-2xl font-bold text-gray-800">{items.length}</p>
              <p className="text-xs text-gray-500 mt-1">Types d'articles</p>
            </div>
            <Package className="h-8 w-8 text-blue-600" />
          </div>
        </div>
        
        <div className="bg-white p-6 rounded-xl shadow-sm border border-gray-200">
          <div className="flex items-center justify-between">
            <div>
              <p className="text-sm font-medium text-gray-600">Unités en Stock</p>
              <p className="text-2xl font-bold text-green-600">{totalQuantity}</p>
              <p className="text-xs text-gray-500 mt-1">Total des quantités</p>
            </div>
            <Package className="h-8 w-8 text-green-600" />
          </div>
        </div>
        
        <div className="bg-white p-6 rounded-xl shadow-sm border border-gray-200">
          <div className="flex items-center justify-between">
            <div>
              <p className="text-sm font-medium text-gray-600">Valeur Totale</p>
              <p className="text-2xl font-bold text-purple-600">{formatCurrency(totalValueMAD, 'MAD')}</p>
              <p className="text-xs text-gray-500 mt-1">Toutes devises en MAD</p>
            </div>
            <Package className="h-8 w-8 text-purple-600" />
          </div>
        </div>
      </div>

      {/* Inventory Table */}
      <div className="bg-white shadow-sm rounded-lg overflow-hidden">
        <table className="min-w-full divide-y divide-gray-200">
          <thead className="bg-gray-50">
            <tr>
              <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase">
                Article
              </th>
              <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase">
                Catégorie
              </th>
              <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase">
                Quantité
              </th>
              <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase">
                Prix Unitaire
              </th>
              <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase">
                Valeur Totale
              </th>
              <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase">
                Actions
              </th>
            </tr>
          </thead>
          <tbody className="bg-white divide-y divide-gray-200">
            {items.map((item) => {
              const itemCurrency = item.currency || 'MAD';
              const totalValue = (item.quantity || 0) * (item.unit_price || 0);
              
              return (
                <tr key={item.id}>
                  <td className="px-6 py-4 whitespace-nowrap">
                    <div>
                      <div className="text-sm font-medium text-gray-900">{item.name}</div>
                      <div className="text-xs text-gray-500">ID: {item.id}</div>
                    </div>
                  </td>
                  <td className="px-6 py-4 whitespace-nowrap text-sm text-gray-900">
                    <span className="px-2 py-1 bg-blue-100 text-blue-800 text-xs font-medium rounded-full">
                      {item.category || 'Non catégorisé'}
                    </span>
                  </td>
                  <td className="px-6 py-4 whitespace-nowrap">
                    <div className="text-sm font-medium text-gray-900">{item.quantity}</div>
                    <div className="text-xs text-gray-500">unités</div>
                  </td>
                  <td className="px-6 py-4 whitespace-nowrap">
                    <div className="text-sm font-medium text-gray-900">
                      {formatCurrency(item.unit_price, itemCurrency)}
                    </div>
                    <div className="text-xs text-gray-500">par unité</div>
                  </td>
                  <td className="px-6 py-4 whitespace-nowrap">
                    <div className="text-sm font-medium text-gray-900">
                      {formatCurrency(totalValue, itemCurrency)}
                    </div>
                    {itemCurrency !== 'MAD' && (
                      <div className="text-xs text-gray-500">
                        = {formatCurrency(convertToMAD(totalValue, itemCurrency), 'MAD')}
                      </div>
                    )}
                  </td>
                  <td className="px-6 py-4 whitespace-nowrap text-sm text-gray-500">
                    <div className="flex gap-2">
                      <button 
                        onClick={() => openModal(item)}
                        className="text-blue-600 hover:text-blue-800 transition-colors"
                        title="Modifier"
                      >
                        <Edit className="h-4 w-4" />
                      </button>
                      <button 
                        onClick={() => handleDelete(item.id, item.name)}
                        className="text-red-600 hover:text-red-800 transition-colors"
                        title="Supprimer"
                      >
                        <Trash2 className="h-4 w-4" />
                      </button>
                    </div>
                  </td>
                </tr>
              );
            })}
          </tbody>
        </table>

        {items.length === 0 && (
          <div className="text-center py-12">
            <Package className="mx-auto h-12 w-12 text-gray-400" />
            <h3 className="mt-2 text-sm font-medium text-gray-900">Aucun article en stock</h3>
            <p className="mt-1 text-sm text-gray-500">Commencez par ajouter des articles à votre inventaire.</p>
            <div className="mt-6">
              <button
                onClick={() => openModal()}
                className="inline-flex items-center px-4 py-2 border border-transparent shadow-sm text-sm font-medium rounded-md text-white bg-blue-600 hover:bg-blue-700"
              >
                <Plus className="-ml-1 mr-2 h-5 w-5" />
                Nouvel Article
              </button>
            </div>
          </div>
        )}
      </div>

      {/* Create/Edit Modal */}
      {showModal && (
        <div className="fixed inset-0 bg-black bg-opacity-50 flex items-center justify-center z-50 p-4">
          <div className="bg-white rounded-lg p-6 w-full max-w-lg">
            <div className="flex justify-between items-center mb-4">
              <h3 className="text-lg font-semibold">
                {editingItem ? 'Modifier Article' : 'Nouvel Article'}
              </h3>
              <button 
                onClick={closeModal} 
                className="text-gray-500 hover:text-gray-700 transition-colors"
                disabled={submitting}
              >
                <X className="h-5 w-5" />
              </button>
            </div>

            <form onSubmit={handleSubmit} className="space-y-4">
              <div>
                <label className="block text-sm font-medium text-gray-700 mb-1">
                  Nom de l'article *
                </label>
                <input
                  type="text"
                  name="name"
                  value={formData.name}
                  onChange={handleInputChange}
                  className="w-full border border-gray-300 rounded px-3 py-2 focus:ring-2 focus:ring-blue-500 focus:border-blue-500"
                  placeholder="Ex: Ordinateur portable, Chaise de bureau, Stylo..."
                  required
                  disabled={submitting}
                />
              </div>

              <div>
                <label className="block text-sm font-medium text-gray-700 mb-1">Catégorie</label>
                <select
                  name="category"
                  value={formData.category}
                  onChange={handleCategoryChange}
                  className="w-full border border-gray-300 rounded px-3 py-2 focus:ring-2 focus:ring-blue-500 focus:border-blue-500"
                  disabled={submitting}
                >
                  <option value="">Sélectionner une catégorie</option>
                  {allCategories.map(category => (
                    <option key={category} value={category}>{category}</option>
                  ))}
                  <option value="Autres">Autre (personnalisé)</option>
                </select>
              </div>

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
                    placeholder="Ex: Équipements électroniques, Matériaux de construction..."
                    required
                    disabled={submitting}
                  />
                  <p className="text-xs text-blue-600 mt-1">
                    Cette catégorie sera sauvegardée pour les futurs articles.
                  </p>
                </div>
              )}

              <div className="grid grid-cols-2 gap-4">
                <div>
                  <label className="block text-sm font-medium text-gray-700 mb-1">
                    Quantité en stock *
                  </label>
                  <input
                    type="number"
                    name="quantity"
                    value={formData.quantity}
                    onChange={handleInputChange}
                    className="w-full border border-gray-300 rounded px-3 py-2 focus:ring-2 focus:ring-blue-500 focus:border-blue-500"
                    placeholder="10"
                    min="0"
                    step="1"
                    required
                    disabled={submitting}
                  />
                  <p className="text-xs text-gray-500 mt-1">Nombre d'unités</p>
                </div>
                
                <div>
                  <label className="block text-sm font-medium text-gray-700 mb-1">
                    Devise du prix
                  </label>
                  <select
                    name="currency"
                    value={formData.currency}
                    onChange={handleInputChange}
                    className="w-full border border-gray-300 rounded px-3 py-2 focus:ring-2 focus:ring-blue-500 focus:border-blue-500"
                    disabled={submitting}
                  >
                    {currencies.map(currency => (
                      <option key={currency.code} value={currency.code}>
                        {currency.symbol} {currency.name}
                      </option>
                    ))}
                  </select>
                </div>
              </div>

              <div>
                <label className="block text-sm font-medium text-gray-700 mb-1">
                  Prix unitaire *
                </label>
                <input
                  type="number"
                  step="0.01"
                  name="unit_price"
                  value={formData.unit_price}
                  onChange={handleInputChange}
                  className="w-full border border-gray-300 rounded px-3 py-2 focus:ring-2 focus:ring-blue-500 focus:border-blue-500"
                  placeholder="15.00"
                  min="0"
                  required
                  disabled={submitting}
                />
                <p className="text-xs text-gray-500 mt-1">
                  Prix par unité en {currencies.find(c => c.code === formData.currency)?.name}
                </p>
              </div>

              {/* Calculation Preview */}
              {formData.quantity && formData.unit_price && (
                <div className="bg-gray-50 border border-gray-200 p-4 rounded-lg">
                  <h4 className="font-medium text-gray-900 mb-2">Aperçu du calcul :</h4>
                  <div className="space-y-1 text-sm text-gray-600">
                    <div>Quantité: <span className="font-medium">{formData.quantity} unités</span></div>
                    <div>Prix unitaire: <span className="font-medium">
                      {formatCurrency(parseFloat(formData.unit_price || 0), formData.currency)}/unité
                    </span></div>
                    <div className="border-t pt-2 mt-2">
                      <div className="font-semibold text-gray-900">
                        Valeur totale: {formatCurrency(
                          parseInt(formData.quantity || 0) * parseFloat(formData.unit_price || 0), 
                          formData.currency
                        )}
                      </div>
                      {formData.currency !== 'MAD' && (
                        <div className="text-xs text-gray-500">
                          Équivalent MAD: {formatCurrency(
                            convertToMAD(
                              parseInt(formData.quantity || 0) * parseFloat(formData.unit_price || 0), 
                              formData.currency
                            ), 
                            'MAD'
                          )}
                        </div>
                      )}
                    </div>
                  </div>
                </div>
              )}

              <div className="flex gap-3 pt-4">
                <button
                  type="submit"
                  disabled={submitting}
                  className="flex-1 bg-blue-600 hover:bg-blue-700 disabled:bg-blue-400 text-white py-2 px-4 rounded-lg flex items-center justify-center gap-2 transition-colors"
                >
                  {submitting ? (
                    <div className="animate-spin rounded-full h-4 w-4 border-b-2 border-white"></div>
                  ) : (
                    <Save className="h-4 w-4" />
                  )}
                  {submitting ? 'Sauvegarde...' : (editingItem ? 'Mettre à jour' : 'Créer')}
                </button>
                <button
                  type="button"
                  onClick={closeModal}
                  disabled={submitting}
                  className="flex-1 bg-gray-300 hover:bg-gray-400 disabled:bg-gray-200 text-gray-800 py-2 px-4 rounded-lg transition-colors"
                >
                  Annuler
                </button>
              </div>
            </form>
          </div>
        </div>
      )}
    </div>
  );
};

export default Inventory;