import React, { useState } from 'react';
import { X, Info } from 'lucide-react';
import { motion, AnimatePresence } from 'framer-motion';
import { useSwipeable } from 'react-swipeable';
import { helpSections } from './helpContent';

const HelpPanel = ({ isOpen, onClose }) => {
  const [activeTab, setActiveTab] = useState('dashboard');

  const handlers = useSwipeable({
    onSwipedDown: onClose,
    preventDefaultTouchmoveEvent: true,
    trackMouse: false,
  });

  if (!isOpen) return null;

  return (
    <AnimatePresence>
      <motion.div
        initial={{ opacity: 0 }}
        animate={{ opacity: 1 }}
        exit={{ opacity: 0 }}
        className="fixed inset-0 z-50 overflow-y-auto"
        onClick={onClose}
      >
        <div className="fixed inset-0 bg-gradient-to-br from-blue-50 via-indigo-50 to-purple-50 opacity-90"></div>
        <div className="relative min-h-screen flex items-end sm:items-center justify-center p-4 sm:p-0">
          <motion.div
            {...handlers}
            initial={{ y: '100%', scale: 0.95 }}
            animate={{ y: 0, scale: 1 }}
            exit={{ y: '100%', scale: 0.95 }}
            transition={{ type: 'spring', damping: 25, stiffness: 300 }}
            className="bg-white rounded-t-lg sm:rounded-xl shadow-2xl w-full max-w-2xl max-h-[90vh] overflow-hidden flex flex-col"
            onClick={(e) => e.stopPropagation()}
          >
            {/* Header */}
            <div className="bg-gradient-to-r from-blue-600 to-indigo-600 text-white p-4 sm:p-6 flex justify-between items-center">
              <div className="flex items-center gap-3">
                <div className="p-2 bg-white bg-opacity-20 rounded-lg">
                  <Info className="h-6 w-6" />
                </div>
                <h2 className="text-xl font-bold">Guide d’utilisation</h2>
              </div>
              <button
                onClick={onClose}
                className="text-white hover:text-blue-100 transition-colors"
                aria-label="Fermer"
              >
                <X className="h-6 w-6" />
              </button>
            </div>

            {/* Tabs */}
            <div className="border-b border-gray-200">
              <nav className="flex overflow-x-auto px-4 py-2 gap-1 scrollbar-hide">
                {Object.entries(helpSections).map(([key, section]) => (
                  <motion.button
                    key={key}
                    whileHover={{ scale: 1.03 }}
                    whileTap={{ scale: 0.98 }}
                    onClick={() => setActiveTab(key)}
                    className={`px-3 py-2 text-sm font-medium rounded-lg whitespace-nowrap transition-all ${
                      activeTab === key
                        ? 'bg-blue-100 text-blue-700 shadow-sm'
                        : 'text-gray-600 hover:bg-gray-100'
                    }`}
                  >
                    {section.icon} {section.title}
                  </motion.button>
                ))}
              </nav>
            </div>

            {/* Content */}
            <motion.div
              key={activeTab}
              initial={{ opacity: 0 }}
              animate={{ opacity: 1 }}
              exit={{ opacity: 0 }}
              transition={{ duration: 0.2 }}
              className="flex-1 overflow-y-auto p-4 sm:p-6"
            >
              <div
                className="prose prose-sm max-w-none text-gray-700"
                dangerouslySetInnerHTML={{ __html: helpSections[activeTab].content }}
              />
            </motion.div>

            {/* Footer */}
            <div className="bg-gray-50 px-4 py-3 text-center text-xs text-gray-500 border-t">
            
              • Guide d’utilisation v1.0 •
            
            </div>
          </motion.div>
        </div>
      </motion.div>
    </AnimatePresence>
  );
};

export default HelpPanel;