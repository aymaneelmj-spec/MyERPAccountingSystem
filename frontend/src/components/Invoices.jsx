import React, { useState, useEffect } from 'react';
import { Plus, Eye, Download, Search, Edit, Trash2, X, Save, FileText, User, Calendar } from 'lucide-react';
import { toast } from 'react-hot-toast';

const Invoices = ({ user, apiService }) => {
  const [invoices, setInvoices] = useState([]);
  const [isLoading, setIsLoading] = useState(true);
  const [searchTerm, setSearchTerm] = useState('');
  const [statusFilter, setStatusFilter] = useState('all');
  const [showModal, setShowModal] = useState(false);
  const [showViewModal, setShowViewModal] = useState(false);
  const [editingInvoice, setEditingInvoice] = useState(null);
  const [viewingInvoice, setViewingInvoice] = useState(null);
  const [submitting, setSubmitting] = useState(false);
  const [formData, setFormData] = useState({
    invoice_number: '',
    client_name: '',
    total_amount: '',
    status: 'pending',
    date_created: new Date().toISOString().split('T')[0],
    company_id: user?.company_id || 1
  });

  useEffect(() => {
    loadInvoices();
  }, [user]);

  const loadInvoices = async () => {
  try {
    setIsLoading(true);
    
    let data;
    try {
      data = await apiService.getInvoices(user?.company_id || 1);
    } catch (apiError) {
      console.error('API Error loading invoices:', apiError);
      
      // ADD FALLBACK
      toast.error('Mode démonstration - Erreur serveur');
      data = [
        {
          id: 1, invoice_number: 'FAC-2024-001', client_name: 'Client Demo',
          total_amount: 1500, status: 'pending', 
          date_created: '2024-01-15', company_id: user?.company_id || 1
        },
        {
          id: 2, invoice_number: 'FAC-2024-002', client_name: 'Société ABC',
          total_amount: 2300, status: 'paid',
          date_created: '2024-01-10', company_id: user?.company_id || 1
        }
      ];
    }
    
    setInvoices(Array.isArray(data) ? data : []);
  } catch (error) {
    console.error('Error loading invoices:', error);
    toast.error('Erreur lors du chargement des factures');
    setInvoices([]);
  } finally {
    setIsLoading(false);
  }
};
  const openModal = (invoice = null) => {
    setEditingInvoice(invoice);
    if (invoice) {
      setFormData({
        invoice_number: invoice.invoice_number,
        client_name: invoice.client_name,
        total_amount: invoice.total_amount.toString(),
        status: invoice.status,
        date_created: invoice.date_created,
        company_id: user?.company_id || 1
      });
    } else {
      setFormData({
        invoice_number: '',
        client_name: '',
        total_amount: '',
        status: 'pending',
        date_created: new Date().toISOString().split('T')[0],
        company_id: user?.company_id || 1
      });
    }
    setShowModal(true);
  };

  const closeModal = () => {
    setShowModal(false);
    setEditingInvoice(null);
    setSubmitting(false);
  };

  // FONCTION POUR VOIR LA FACTURE
  const handleViewInvoice = (invoice) => {
    setViewingInvoice(invoice);
    setShowViewModal(true);
  };

  // FONCTION POUR TÉLÉCHARGER LA FACTURE EN PDF
  const handleDownloadInvoice = (invoice) => {
    try {
      // Générer le contenu HTML professionnel de la facture
      const invoiceHTML = generateInvoiceHTML(invoice);
      
      // Créer un blob et télécharger
      const blob = new Blob([invoiceHTML], { type: 'text/html' });
      const url = URL.createObjectURL(blob);
      const link = document.createElement('a');
      link.href = url;
      link.download = `Facture-${invoice.invoice_number}-${invoice.client_name.replace(/\s+/g, '-')}.html`;
      document.body.appendChild(link);
      link.click();
      document.body.removeChild(link);
      URL.revokeObjectURL(url);
      
      // Également ouvrir dans une nouvelle fenêtre pour impression PDF
      const printWindow = window.open('', '_blank');
      printWindow.document.write(invoiceHTML);
      printWindow.document.close();
      
      toast.success('Facture téléchargée avec succès !');
    } catch (error) {
      console.error('Erreur téléchargement facture:', error);
      toast.error('Erreur lors du téléchargement');
    }
  };

  // GÉNÉRER HTML PROFESSIONNEL POUR LA FACTURE
  const generateInvoiceHTML = (invoice) => {
    const now = new Date();
    return `<!DOCTYPE html>
<html>
<head>
    <meta charset="utf-8">
    <title>Facture ${invoice.invoice_number}</title>
    <style>
        @page { margin: 20mm; size: A4; }
        body { font-family: 'Arial', sans-serif; line-height: 1.6; color: #333; margin: 0; padding: 20px; }
        .header { display: flex; justify-content: space-between; align-items: flex-start; margin-bottom: 40px; border-bottom: 3px solid #2563eb; padding-bottom: 20px; }
        .company-logo { width: 80px; height: 80px; background: linear-gradient(135deg, #2563eb, #1d4ed8); border-radius: 12px; display: flex; align-items: center; justify-content: center; color: white; font-size: 24px; font-weight: bold; }
        .company-info { text-align: right; }
        .company-name { font-size: 28px; font-weight: bold; color: #2563eb; margin: 0; }
        .company-details { font-size: 14px; color: #666; margin: 5px 0; }
        .invoice-title { text-align: center; margin: 30px 0; }
        .invoice-title h1 { font-size: 36px; color: #1f2937; margin: 0; text-transform: uppercase; letter-spacing: 2px; }
        .invoice-number { color: #2563eb; font-size: 18px; margin-top: 10px; }
        .invoice-details { display: flex; justify-content: space-between; margin: 30px 0; }
        .client-info, .invoice-info { flex: 1; padding: 20px; background: #f8fafc; border-radius: 10px; margin: 0 10px; }
        .client-info h3, .invoice-info h3 { color: #374151; margin-bottom: 10px; font-size: 16px; text-transform: uppercase; letter-spacing: 1px; }
        .amount-section { background: linear-gradient(135deg, #f0f9ff, #e0f2fe); padding: 30px; border-radius: 15px; margin: 30px 0; text-align: center; border: 2px solid #0ea5e9; }
        .amount-label { font-size: 18px; color: #0f172a; margin-bottom: 10px; }
        .amount-value { font-size: 48px; font-weight: bold; color: #0ea5e9; margin: 10px 0; }
        .amount-words { font-size: 14px; color: #64748b; font-style: italic; }
        .status-badge { display: inline-block; padding: 8px 16px; border-radius: 20px; font-size: 12px; font-weight: bold; text-transform: uppercase; }
        .status-pending { background: #fef3c7; color: #92400e; }
        .status-paid { background: #d1fae5; color: #065f46; }
        .status-overdue { background: #fee2e2; color: #991b1b; }
        .footer { margin-top: 50px; text-align: center; font-size: 12px; color: #6b7280; border-top: 1px solid #e5e7eb; padding-top: 20px; }
        .payment-info { background: #f9fafb; padding: 20px; border-radius: 10px; margin: 30px 0; border-left: 4px solid #10b981; }
    </style>
</head>
<body>
    <div class="header">
        <div>
            <div class="company-logo">HD</div>
        </div>
        <div class="company-info">
            <h2 class="company-name">HAPPY DEAL TRANSIT</h2>
            <div class="company-details">
                9, Plateaux ESSALAM, Casablanca<br>
                Tél: +212 5 22 20 85 94<br>
                Email: contact@hdtransit.com<br>
                RC: 123456 - IF: 7890123
            </div>
        </div>
    </div>
    
    <div class="invoice-title">
        <h1>FACTURE</h1>
        <div class="invoice-number">N° ${invoice.invoice_number}</div>
    </div>
    
    <div class="invoice-details">
        <div class="client-info">
            <h3>Facturé à:</h3>
            <div style="font-size: 18px; font-weight: bold; color: #1f2937;">${invoice.client_name}</div>
        </div>
        <div class="invoice-info">
            <h3>Informations Facture:</h3>
            <div><strong>Date:</strong> ${new Date(invoice.date_created).toLocaleDateString('fr-FR')}</div>
            <div><strong>Échéance:</strong> ${new Date(new Date(invoice.date_created).getTime() + 30*24*60*60*1000).toLocaleDateString('fr-FR')}</div>
            <div><strong>Statut:</strong> <span class="status-badge status-${invoice.status}">
                ${invoice.status === 'paid' ? 'Payée' : invoice.status === 'pending' ? 'En attente' : 'En retard'}
            </span></div>
        </div>
    </div>
    
    <div class="amount-section">
        <div class="amount-label">MONTANT TOTAL À PAYER</div>
        <div class="amount-value">${invoice.total_amount.toLocaleString('fr-FR')} د.م.</div>
        <div class="amount-words">${numberToWords(invoice.total_amount)} dirhams</div>
    </div>
    
    ${invoice.status === 'pending' || invoice.status === 'overdue' ? `
    <div class="payment-info">
        <h3 style="color: #065f46; margin-bottom: 10px;">Informations de Paiement</h3>
        <p><strong>Virement Bancaire:</strong> BMCE Bank - RIB: 011 780 000012345678 90</p>
        <p><strong>Chèque:</strong> À l'ordre de "Happy Deal Transit"</p>
        <p><strong>Espèces:</strong> À nos bureaux aux heures d'ouverture</p>
    </div>
    ` : ''}
    
    <div class="footer">
        <p><strong>Merci pour votre confiance !</strong></p>
        <p>Cette facture a été générée automatiquement le ${now.toLocaleDateString('fr-FR')} à ${now.toLocaleTimeString('fr-FR')}</p>
        <p>Happy Deal Transit - Votre partenaire logistique de confiance</p>
    </div>
</body>
</html>`;
  };

  // CONVERTIR NOMBRE EN MOTS (FRANÇAIS)
  const numberToWords = (num) => {
    if (num === 0) return 'zéro';
    
    const ones = ['', 'un', 'deux', 'trois', 'quatre', 'cinq', 'six', 'sept', 'huit', 'neuf'];
    const teens = ['dix', 'onze', 'douze', 'treize', 'quatorze', 'quinze', 'seize', 'dix-sept', 'dix-huit', 'dix-neuf'];
    const tens = ['', '', 'vingt', 'trente', 'quarante', 'cinquante', 'soixante', 'soixante-dix', 'quatre-vingt', 'quatre-vingt-dix'];
    const thousands = ['', 'mille', 'million', 'milliard'];
    
    if (num < 10) return ones[num];
    if (num < 20) return teens[num - 10];
    if (num < 100) {
      const ten = Math.floor(num / 10);
      const one = num % 10;
      return tens[ten] + (one > 0 ? '-' + ones[one] : '');
    }
    if (num < 1000) {
      const hundred = Math.floor(num / 100);
      const rest = num % 100;
      return (hundred > 1 ? ones[hundred] + ' ' : '') + 'cent' + (rest > 0 ? ' ' + numberToWords(rest) : '');
    }
    
    return 'nombre complexe'; // Simplification pour l'exemple
  };

  const handleSubmit = async () => {
    // Validation
    if (!formData.client_name.trim()) {
      toast.error('Le nom du client est requis');
      return;
    }
    
    if (!formData.total_amount || parseFloat(formData.total_amount) <= 0) {
      toast.error('Le montant doit être supérieur à 0');
      return;
    }

    setSubmitting(true);

    try {
      const submitData = {
        ...formData,
        total_amount: parseFloat(formData.total_amount)
      };

      if (editingInvoice) {
        await apiService.updateInvoice(editingInvoice.id, submitData);
        toast.success('Facture mise à jour avec succès !');
        
        // Mettre à jour l'état local
        setInvoices(invoices.map(invoice => 
          invoice.id === editingInvoice.id 
            ? { ...invoice, ...submitData }
            : invoice
        ));
      } else {
        const result = await apiService.createInvoice(submitData);
        toast.success('Facture créée avec succès !');
        
        // Ajouter à l'état local
        const newInvoice = {
          ...submitData,
          id: result.id || Date.now(),
          invoice_number: result.invoice_number || `FAC-${Date.now()}`
        };
        setInvoices([newInvoice, ...invoices]);
      }
      
      closeModal();
    } catch (error) {
      console.error('Erreur sauvegarde facture:', error);
      toast.error('Erreur lors de la sauvegarde');
    } finally {
      setSubmitting(false);
    }
  };

  const handleDelete = async (id, invoiceNumber) => {
    if (window.confirm(`Êtes-vous sûr de vouloir supprimer la facture "${invoiceNumber}" ?`)) {
      try {
        await apiService.deleteInvoice(id);
        toast.success('Facture supprimée avec succès !');
        setInvoices(invoices.filter(invoice => invoice.id !== id));
      } catch (error) {
        console.error('Erreur suppression facture:', error);
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

  const filteredInvoices = invoices.filter(invoice => {
    const matchesSearch = invoice.client_name.toLowerCase().includes(searchTerm.toLowerCase()) ||
                         invoice.invoice_number.toLowerCase().includes(searchTerm.toLowerCase());
    const matchesStatus = statusFilter === 'all' || invoice.status === statusFilter;
    return matchesSearch && matchesStatus;
  });

  const getStatusColor = (status) => {
    switch (status) {
      case 'paid':
        return 'bg-green-100 text-green-800';
      case 'pending':
        return 'bg-yellow-100 text-yellow-800';
      case 'overdue':
        return 'bg-red-100 text-red-800';
      default:
        return 'bg-gray-100 text-gray-800';
    }
  };

  const getStatusLabel = (status) => {
    switch (status) {
      case 'paid':
        return 'Payée';
      case 'pending':
        return 'En attente';
      case 'overdue':
        return 'En retard';
      default:
        return status;
    }
  };

  if (isLoading) {
    return (
      <div className="flex items-center justify-center h-64">
        <div className="animate-spin rounded-full h-32 w-32 border-b-2 border-blue-600"></div>
      </div>
    );
  }

  return (
    <div className="space-y-6">
      <div className="sm:flex sm:items-center justify-between">
        <div className="sm:flex-auto">
          <h1 className="text-2xl font-bold text-gray-900">Factures</h1>
          <p className="mt-2 text-sm text-gray-700">
            Gérer vos factures clients et la facturation
          </p>
        </div>
        <div className="mt-4 sm:mt-0 sm:ml-16 sm:flex-none">
          <button
            onClick={() => openModal()}
            className="inline-flex items-center justify-center rounded-md border border-transparent bg-blue-600 px-4 py-2 text-sm font-medium text-white shadow-sm hover:bg-blue-700 focus:outline-none focus:ring-2 focus:ring-blue-500 focus:ring-offset-2 sm:w-auto"
          >
            <Plus className="h-4 w-4 mr-2" />
            Créer Facture
          </button>
        </div>
      </div>

      {/* Filtres */}
      <div className="mb-6 flex flex-col sm:flex-row gap-4">
        <div className="relative flex-1">
          <Search className="absolute left-3 top-1/2 transform -translate-y-1/2 text-gray-400 h-5 w-5" />
          <input
            type="text"
            placeholder="Rechercher factures..."
            className="pl-10 pr-4 py-2 border border-gray-300 rounded-md focus:ring-blue-500 focus:border-blue-500 block w-full"
            value={searchTerm}
            onChange={(e) => setSearchTerm(e.target.value)}
          />
        </div>
        <select
          className="px-3 py-2 border border-gray-300 rounded-md focus:ring-blue-500 focus:border-blue-500"
          value={statusFilter}
          onChange={(e) => setStatusFilter(e.target.value)}
        >
          <option value="all">Tous Statuts</option>
          <option value="pending">En attente</option>
          <option value="paid">Payées</option>
          <option value="overdue">En retard</option>
        </select>
      </div>

      {/* Tableau des Factures */}
      <div className="overflow-hidden shadow ring-1 ring-black ring-opacity-5 md:rounded-lg">
        <table className="min-w-full divide-y divide-gray-300">
          <thead className="bg-gray-50">
            <tr>
              <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wide">
                Facture #
              </th>
              <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wide">
                Client
              </th>
              <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wide">
                Date
              </th>
              <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wide">
                Montant
              </th>
              <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wide">
                Statut
              </th>
              <th className="px-6 py-3 text-right text-xs font-medium text-gray-500 uppercase tracking-wide">
                Actions
              </th>
            </tr>
          </thead>
          <tbody className="bg-white divide-y divide-gray-200">
            {filteredInvoices.map((invoice) => (
              <tr key={invoice.id}>
                <td className="px-6 py-4 whitespace-nowrap text-sm font-medium text-gray-900">
                  {invoice.invoice_number}
                </td>
                <td className="px-6 py-4 whitespace-nowrap text-sm text-gray-900">
                  {invoice.client_name}
                </td>
                <td className="px-6 py-4 whitespace-nowrap text-sm text-gray-500">
                  {new Date(invoice.date_created).toLocaleDateString('fr-FR')}
                </td>
                <td className="px-6 py-4 whitespace-nowrap text-sm font-medium text-gray-900">
                  {invoice.total_amount.toLocaleString()} د.م.
                </td>
                <td className="px-6 py-4 whitespace-nowrap">
                  <span className={`inline-flex items-center px-2.5 py-0.5 rounded-full text-xs font-medium ${getStatusColor(invoice.status)}`}>
                    {getStatusLabel(invoice.status)}
                  </span>
                </td>
                <td className="px-6 py-4 whitespace-nowrap text-right text-sm font-medium">
                  <div className="flex justify-end gap-2">
                    <button
                      onClick={() => openModal(invoice)}
                      className="text-blue-600 hover:text-blue-900"
                      title="Modifier"
                    >
                      <Edit className="h-4 w-4" />
                    </button>
                    <button
                      onClick={() => handleDelete(invoice.id, invoice.invoice_number)}
                      className="text-red-600 hover:text-red-900"
                      title="Supprimer"
                    >
                      <Trash2 className="h-4 w-4" />
                    </button>
                    <button 
                      onClick={() => handleViewInvoice(invoice)}
                      className="text-indigo-600 hover:text-indigo-900" 
                      title="Voir la facture"
                    >
                      <Eye className="h-4 w-4" />
                    </button>
                    <button 
                      onClick={() => handleDownloadInvoice(invoice)}
                      className="text-gray-600 hover:text-gray-900" 
                      title="Télécharger PDF"
                    >
                      <Download className="h-4 w-4" />
                    </button>
                  </div>
                </td>
              </tr>
            ))}
          </tbody>
        </table>

        {filteredInvoices.length === 0 && (
          <div className="text-center py-12">
            <FileText className="mx-auto h-12 w-12 text-gray-400" />
            <h3 className="mt-2 text-sm font-medium text-gray-900">Aucune facture</h3>
            <p className="mt-1 text-sm text-gray-500">
              {invoices.length === 0 ? 'Commencez par créer votre première facture.' : 'Aucun résultat pour votre recherche.'}
            </p>
            {invoices.length === 0 && (
              <div className="mt-6">
                <button
                  onClick={() => openModal()}
                  className="inline-flex items-center px-4 py-2 border border-transparent shadow-sm text-sm font-medium rounded-md text-white bg-blue-600 hover:bg-blue-700"
                >
                  <Plus className="-ml-1 mr-2 h-5 w-5" />
                  Créer Facture
                </button>
              </div>
            )}
          </div>
        )}
      </div>

      {/* Cartes Résumé */}
      <div className="mt-8 grid grid-cols-1 gap-5 sm:grid-cols-2 lg:grid-cols-4">
        <div className="bg-white overflow-hidden shadow rounded-lg">
          <div className="p-5">
            <div className="flex items-center">
              <div className="w-0 flex-1">
                <dl>
                  <dt className="text-sm font-medium text-gray-500 truncate">
                    Total Factures
                  </dt>
                  <dd className="text-lg font-medium text-gray-900">
                    {invoices.length}
                  </dd>
                </dl>
              </div>
            </div>
          </div>
        </div>

        <div className="bg-white overflow-hidden shadow rounded-lg">
          <div className="p-5">
            <div className="flex items-center">
              <div className="w-0 flex-1">
                <dl>
                  <dt className="text-sm font-medium text-gray-500 truncate">
                    En attente
                  </dt>
                  <dd className="text-lg font-medium text-yellow-600">
                    {invoices.filter(i => i.status === 'pending').length}
                  </dd>
                </dl>
              </div>
            </div>
          </div>
        </div>

        <div className="bg-white overflow-hidden shadow rounded-lg">
          <div className="p-5">
            <div className="flex items-center">
              <div className="w-0 flex-1">
                <dl>
                  <dt className="text-sm font-medium text-gray-500 truncate">
                    Payées
                  </dt>
                  <dd className="text-lg font-medium text-green-600">
                    {invoices.filter(i => i.status === 'paid').length}
                  </dd>
                </dl>
              </div>
            </div>
          </div>
        </div>

        <div className="bg-white overflow-hidden shadow rounded-lg">
          <div className="p-5">
            <div className="flex items-center">
              <div className="w-0 flex-1">
                <dl>
                  <dt className="text-sm font-medium text-gray-500 truncate">
                    Valeur Totale
                  </dt>
                  <dd className="text-lg font-medium text-gray-900">
                    {invoices.reduce((sum, inv) => sum + inv.total_amount, 0).toLocaleString()} د.م.
                  </dd>
                </dl>
              </div>
            </div>
          </div>
        </div>
      </div>

      {/* Modal Créer/Modifier */}
      {showModal && (
        <div className="fixed inset-0 bg-black bg-opacity-50 flex items-center justify-center z-50 p-4">
          <div className="bg-white rounded-lg p-6 w-full max-w-md">
            <div className="flex justify-between items-center mb-4">
              <h3 className="text-lg font-semibold">
                {editingInvoice ? 'Modifier Facture' : 'Nouvelle Facture'}
              </h3>
              <button 
                onClick={closeModal} 
                className="text-gray-500 hover:text-gray-700 transition-colors"
                disabled={submitting}
              >
                <X className="h-5 w-5" />
              </button>
            </div>

            <div className="space-y-4">
              <div>
                <label className="block text-sm font-medium text-gray-700 mb-1">
                  Numéro Facture
                </label>
                <input
                  type="text"
                  name="invoice_number"
                  value={formData.invoice_number}
                  onChange={handleInputChange}
                  className="w-full border border-gray-300 rounded px-3 py-2 focus:ring-2 focus:ring-blue-500 focus:border-blue-500"
                  placeholder="Laissez vide pour génération auto"
                  disabled={submitting}
                />
              </div>

              <div>
                <label className="block text-sm font-medium text-gray-700 mb-1">
                  Nom du Client *
                </label>
                <input
                  type="text"
                  name="client_name"
                  value={formData.client_name}
                  onChange={handleInputChange}
                  className="w-full border border-gray-300 rounded px-3 py-2 focus:ring-2 focus:ring-blue-500 focus:border-blue-500"
                  placeholder="Nom du client"
                  required
                  disabled={submitting}
                />
              </div>

              <div>
                <label className="block text-sm font-medium text-gray-700 mb-1">
                  Montant (د.م.) *
                </label>
                <input
                  type="number"
                  step="0.01"
                  name="total_amount"
                  value={formData.total_amount}
                  onChange={handleInputChange}
                  className="w-full border border-gray-300 rounded px-3 py-2 focus:ring-2 focus:ring-blue-500 focus:border-blue-500"
                  placeholder="0.00"
                  min="0"
                  required
                  disabled={submitting}
                />
              </div>

              <div>
                <label className="block text-sm font-medium text-gray-700 mb-1">Statut</label>
                <select
                  name="status"
                  value={formData.status}
                  onChange={handleInputChange}
                  className="w-full border border-gray-300 rounded px-3 py-2 focus:ring-2 focus:ring-blue-500 focus:border-blue-500"
                  disabled={submitting}
                >
                  <option value="pending">En attente</option>
                  <option value="paid">Payée</option>
                  <option value="overdue">En retard</option>
                </select>
              </div>

              <div>
                <label className="block text-sm font-medium text-gray-700 mb-1">Date de création</label>
                <input
                  type="date"
                  name="date_created"
                  value={formData.date_created}
                  onChange={handleInputChange}
                  className="w-full border border-gray-300 rounded px-3 py-2 focus:ring-2 focus:ring-blue-500 focus:border-blue-500"
                  disabled={submitting}
                />
              </div>

              <div className="flex gap-3 pt-4">
                <button
                  onClick={handleSubmit}
                  disabled={submitting}
                  className="flex-1 bg-blue-600 hover:bg-blue-700 disabled:bg-blue-400 text-white py-2 px-4 rounded-lg flex items-center justify-center gap-2 transition-colors"
                >
                  {submitting ? (
                    <div className="animate-spin rounded-full h-4 w-4 border-b-2 border-white"></div>
                  ) : (
                    <Save className="h-4 w-4" />
                  )}
                  {submitting ? 'Sauvegarde...' : (editingInvoice ? 'Mettre à jour' : 'Créer')}
                </button>
                <button
                  onClick={closeModal}
                  disabled={submitting}
                  className="flex-1 bg-gray-300 hover:bg-gray-400 disabled:bg-gray-200 text-gray-800 py-2 px-4 rounded-lg transition-colors"
                >
                  Annuler
                </button>
              </div>
            </div>
          </div>
        </div>
      )}

      {/* Modal Voir Facture */}
      {showViewModal && viewingInvoice && (
        <div className="fixed inset-0 bg-black bg-opacity-50 flex items-center justify-center z-50 p-4">
          <div className="bg-white rounded-lg p-6 w-full max-w-2xl max-h-[90vh] overflow-y-auto">
            <div className="flex justify-between items-center mb-6">
              <h3 className="text-xl font-semibold">Facture {viewingInvoice.invoice_number}</h3>
              <button 
                onClick={() => setShowViewModal(false)} 
                className="text-gray-500 hover:text-gray-700 transition-colors"
              >
                <X className="h-6 w-6" />
              </button>
            </div>

            {/* Aperçu de la facture */}
            <div className="border border-gray-200 rounded-lg p-6 bg-gray-50">
              {/* En-tête */}
              <div className="flex justify-between items-start mb-8">
                <div>
                  <div className="w-16 h-16 bg-blue-600 rounded-lg flex items-center justify-center text-white font-bold text-xl mb-2">
                    HD
                  </div>
                </div>
                <div className="text-right">
                  <h2 className="text-2xl font-bold text-blue-600">HAPPY DEAL TRANSIT</h2>
                  <p className="text-sm text-gray-600 mt-1">
                    9, Plateaux ESSALAM, Casablanca<br/>
                    Tél: +212 5 22 20 85 94<br/>
                    Email: contact@hdtransit.com
                  </p>
                </div>
              </div>

              {/* Titre facture */}
              <div className="text-center mb-8">
                <h1 className="text-3xl font-bold text-gray-900">FACTURE</h1>
                <p className="text-blue-600 font-semibold">N° {viewingInvoice.invoice_number}</p>
              </div>

              {/* Détails */}
              <div className="grid grid-cols-2 gap-8 mb-8">
                <div className="bg-white p-4 rounded-lg">
                  <h3 className="font-semibold text-gray-900 mb-2">Facturé à:</h3>
                  <p className="text-lg font-medium">{viewingInvoice.client_name}</p>
                </div>
                <div className="bg-white p-4 rounded-lg">
                  <h3 className="font-semibold text-gray-900 mb-2">Informations:</h3>
                  <p><strong>Date:</strong> {new Date(viewingInvoice.date_created).toLocaleDateString('fr-FR')}</p>
                  <p><strong>Statut:</strong> 
                    <span className={`ml-2 px-2 py-1 rounded-full text-xs font-medium ${getStatusColor(viewingInvoice.status)}`}>
                      {getStatusLabel(viewingInvoice.status)}
                    </span>
                  </p>
                </div>
              </div>

              {/* Montant */}
              <div className="bg-blue-50 border-2 border-blue-200 rounded-lg p-6 text-center mb-6">
                <p className="text-lg text-gray-700 mb-2">MONTANT TOTAL À PAYER</p>
                <p className="text-4xl font-bold text-blue-600">{viewingInvoice.total_amount.toLocaleString()} د.م.</p>
              </div>

              {/* Actions dans la modal */}
              <div className="flex justify-center gap-4">
                <button
                  onClick={() => handleDownloadInvoice(viewingInvoice)}
                  className="bg-blue-600 hover:bg-blue-700 text-white px-6 py-2 rounded-lg flex items-center gap-2 transition-colors"
                >
                  <Download className="h-4 w-4" />
                  Télécharger PDF
                </button>
                <button
                  onClick={() => {
                    setShowViewModal(false);
                    openModal(viewingInvoice);
                  }}
                  className="bg-gray-600 hover:bg-gray-700 text-white px-6 py-2 rounded-lg flex items-center gap-2 transition-colors"
                >
                  <Edit className="h-4 w-4" />
                  Modifier
                </button>
              </div>
            </div>
          </div>
        </div>
      )}
    </div>
  );
};

export default Invoices;