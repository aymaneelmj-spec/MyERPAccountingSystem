// src/components/DataImportModal.jsx
import React, { useState, useEffect } from 'react';
import { X, ArrowRight, Plus, AlertCircle } from 'lucide-react';
import { toast } from 'react-hot-toast';

const DataImportModal = ({ isOpen, onClose, file, onImport }) => {
  const [headers, setHeaders] = useState([]);
  const [mapping, setMapping] = useState({});
  const [customColumns, setCustomColumns] = useState({});
  const [isCustomOpen, setIsCustomOpen] = useState(false);
  const [customField, setCustomField] = useState('');

  useEffect(() => {
    if (isOpen && file) {
      const reader = new FileReader();
      reader.onload = (e) => {
        const text = e.target.result;
        const lines = text.split('\n');
        const firstLine = lines[0];
        if (firstLine) {
          const headers = firstLine.split(',').map(h => h.trim());
          setHeaders(headers);
          // Initialize mapping
          const initialMapping = {};
          headers.forEach((header, index) => {
            initialMapping[index] = '';
          });
          setMapping(initialMapping);
        }
      };
      reader.readAsText(file);
    }
  }, [isOpen, file]);

  const handleMappingChange = (index, field) => {
    if (field === 'custom') {
      setIsCustomOpen(true);
      setCustomField('');
      return;
    }
    setMapping(prev => ({
      ...prev,
      [index]: field
    }));
  };

  const handleCustomSubmit = () => {
    if (!customField.trim()) {
      toast.error('Veuillez entrer un nom de champ');
      return;
    }
    setMapping(prev => ({
      ...prev,
      [Object.keys(mapping).find(key => mapping[key] === 'custom')]: customField.trim()
    }));
    setIsCustomOpen(false);
    setCustomField('');
  };

  const handleImport = () => {
    // Validate all required fields are mapped
    const requiredFields = ['date', 'description', 'amount', 'type'];
    const mappedFields = Object.values(mapping);
    const missingRequired = requiredFields.filter(field => !mappedFields.includes(field));

    if (missingRequired.length > 0) {
      toast.error(`Champs requis non mappés: ${missingRequired.join(', ')}`);
      return;
    }

    onImport(mapping);
    onClose();
  };

  if (!isOpen) return null;

  return (
    <div className="fixed inset-0 bg-black bg-opacity-50 flex items-center justify-center z-50 p-4">
      <div className="bg-white rounded-lg p-6 w-full max-w-4xl max-h-[80vh] overflow-y-auto">
        <div className="flex justify-between items-center mb-4">
          <h3 className="text-lg font-semibold">Mapper les colonnes</h3>
          <button onClick={onClose} className="text-gray-500 hover:text-gray-700">
            <X className="h-5 w-5" />
          </button>
        </div>

        <div className="mb-4">
          <p className="text-sm text-gray-600 mb-2">Fichier: {file?.name}</p>
          <p className="text-sm text-gray-600">Les colonnes ci-dessous seront mappées vers vos champs système.</p>
        </div>

        <div className="grid grid-cols-1 md:grid-cols-2 gap-4 mb-4">
          {headers.map((header, index) => (
            <div key={index} className="flex items-center gap-2">
              <div className="bg-gray-100 px-2 py-1 rounded text-sm">
                {header}
              </div>
              <ArrowRight className="h-4 w-4 text-gray-400" />
              <select
                value={mapping[index] || ''}
                onChange={(e) => handleMappingChange(index, e.target.value)}
                className="border border-gray-300 rounded px-2 py-1 text-sm"
              >
                <option value="">Sélectionner...</option>
                <option value="date">Date</option>
                <option value="description">Description</option>
                <option value="amount">Montant</option>
                <option value="type">Type (income/expense)</option>
                <option value="category">Catégorie</option>
                <option value="currency">Devise</option>
                <option value="custom">Autre (personnalisé)</option>
              </select>
            </div>
          ))}
        </div>

        {isCustomOpen && (
          <div className="bg-blue-50 border border-blue-200 rounded-lg p-4 mb-4">
            <div className="flex items-center gap-2 mb-2">
              <Plus className="h-4 w-4 text-blue-500" />
              <h4 className="text-sm font-medium text-blue-700">Ajouter un champ personnalisé</h4>
            </div>
            <input
              type="text"
              value={customField}
              onChange={(e) => setCustomField(e.target.value)}
              className="w-full border border-blue-300 rounded px-2 py-1 text-sm"
              placeholder="Nom du champ (ex: Référence, Projet, Client)"
            />
            <div className="mt-2 flex gap-2">
              <button
                onClick={handleCustomSubmit}
                className="bg-blue-600 hover:bg-blue-700 text-white px-2 py-1 text-xs rounded"
              >
                Ajouter
              </button>
              <button
                onClick={() => setIsCustomOpen(false)}
                className="bg-gray-300 hover:bg-gray-400 text-gray-800 px-2 py-1 text-xs rounded"
              >
                Annuler
              </button>
            </div>
          </div>
        )}

        <div className="bg-yellow-50 border border-yellow-200 rounded-lg p-4 mb-4">
          <div className="flex items-start gap-2">
            <AlertCircle className="h-5 w-5 text-yellow-500 mt-0.5" />
            <div className="text-sm text-yellow-700">
              <strong>Important:</strong> Les champs <code>date</code>, <code>description</code>, <code>amount</code>, et <code>type</code> sont requis pour l'import.
            </div>
          </div>
        </div>

        <div className="flex gap-3 pt-4">
          <button
            onClick={onClose}
            className="flex-1 bg-gray-300 hover:bg-gray-400 text-gray-800 py-2 px-4 rounded-lg transition-colors"
          >
            Annuler
          </button>
          <button
            onClick={handleImport}
            className="flex-1 bg-blue-600 hover:bg-blue-700 text-white py-2 px-4 rounded-lg transition-colors"
          >
            Importer
          </button>
        </div>
      </div>
    </div>
  );
};

export default DataImportModal;