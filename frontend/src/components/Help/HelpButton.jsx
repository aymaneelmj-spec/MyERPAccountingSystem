// src/components/Help/HelpButton.jsx
import React, { useState } from 'react';
import { HelpCircle, X, Book, MessageCircle, Phone, Mail } from 'lucide-react';

const HelpButton = () => {
  const [showHelp, setShowHelp] = useState(false);

  const helpSections = [
    {
      title: 'Guide Rapide',
      icon: Book,
      items: [
        'Navigation: Utilisez le menu à gauche pour accéder aux différentes sections',
        'Transactions: Gérez vos revenus et dépenses avec support multi-devises',
        'Factures: Créez et téléchargez des factures professionnelles',
        'Inventaire: Suivez vos articles et stocks',
        'Rapports: Générez des analyses financières détaillées'
      ]
    },
    {
      title: 'Support',
      icon: MessageCircle,
      items: [
        'Email: support@hdtransit.com',
        'Téléphone: +212 5 22 20 85 94',
        'Horaires: Lun-Ven 9h-17h'
      ]
    }
  ];

  if (!showHelp) {
    return (
      <button
        onClick={() => setShowHelp(true)}
        className="fixed bottom-6 left-6 bg-blue-600 hover:bg-blue-700 text-white p-3 rounded-full shadow-lg transition-colors z-40"
        title="Aide et Support"
      >
        <HelpCircle className="h-6 w-6" />
      </button>
    );
  }

  return (
    <div className="fixed inset-0 bg-black bg-opacity-50 flex items-center justify-center z-50 p-4">
      <div className="bg-white rounded-lg w-full max-w-2xl max-h-[90vh] overflow-hidden">
        <div className="flex justify-between items-center p-6 border-b bg-blue-50">
          <div className="flex items-center gap-3">
            <HelpCircle className="h-6 w-6 text-blue-600" />
            <h2 className="text-xl font-semibold text-gray-900">Centre d'Aide</h2>
          </div>
          <button
            onClick={() => setShowHelp(false)}
            className="text-gray-500 hover:text-gray-700 transition-colors"
          >
            <X className="h-6 w-6" />
          </button>
        </div>

        <div className="p-6 overflow-y-auto max-h-[calc(90vh-120px)] space-y-6">
          {helpSections.map((section, index) => {
            const Icon = section.icon;
            return (
              <div key={index} className="space-y-3">
                <div className="flex items-center gap-2 mb-3">
                  <Icon className="h-5 w-5 text-blue-600" />
                  <h3 className="text-lg font-medium text-gray-900">{section.title}</h3>
                </div>
                <ul className="space-y-2 ml-7">
                  {section.items.map((item, itemIndex) => (
                    <li key={itemIndex} className="text-sm text-gray-600 flex items-start gap-2">
                      <span className="text-blue-500 mt-1">•</span>
                      <span>{item}</span>
                    </li>
                  ))}
                </ul>
              </div>
            );
          })}

          <div className="border-t pt-6 mt-6">
            <h3 className="text-lg font-medium text-gray-900 mb-3">Contact Direct</h3>
            <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
              <a
                href="tel:+212522208594"
                className="flex items-center gap-3 p-3 bg-green-50 rounded-lg hover:bg-green-100 transition-colors"
              >
                <Phone className="h-5 w-5 text-green-600" />
                <div>
                  <div className="text-sm font-medium text-gray-900">Téléphone</div>
                  <div className="text-xs text-gray-600">+212 5 22 20 85 94</div>
                </div>
              </a>
              <a
                href="mailto:support@hdtransit.com"
                className="flex items-center gap-3 p-3 bg-blue-50 rounded-lg hover:bg-blue-100 transition-colors"
              >
                <Mail className="h-5 w-5 text-blue-600" />
                <div>
                  <div className="text-sm font-medium text-gray-900">Email</div>
                  <div className="text-xs text-gray-600">support@hdtransit.com</div>
                </div>
              </a>
            </div>
          </div>

          <div className="bg-yellow-50 border border-yellow-200 rounded-lg p-4">
            <h4 className="text-sm font-medium text-yellow-800 mb-2">Version Démo</h4>
            <p className="text-xs text-yellow-700">
              Cette version fonctionne en mode démonstration. Certaines fonctionnalités 
              utilisent des données simulées si le serveur backend n'est pas disponible.
            </p>
          </div>
        </div>
      </div>
    </div>
  );
};

export default HelpButton;