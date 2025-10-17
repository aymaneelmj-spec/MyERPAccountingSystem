// src/components/Help/helpContent.js

export const helpSections = {
  dashboard: {
    title: "Tableau de Bord",
    icon: "📊",
    content: `
      <p><strong>Vue d’ensemble :</strong> Consultez vos KPIs financiers en temps réel</p>
      <p><strong>💱 Multi-devises :</strong> Changez la devise d’affichage dans les contrôles</p>
      <p><strong>📈 Graphiques :</strong> Analysez vos tendances revenus vs dépenses</p>
      <p><strong>📁 Import rapide :</strong> Glissez-déposez vos fichiers CSV/Excel</p>
      <p><strong>🔄 Actualisation :</strong> Les données se mettent à jour automatiquement</p>
      <p><strong>📱 Mobile-friendly :</strong> Interface responsive sur tous appareils</p>
    `
  },
  transactions: {
    title: "Transactions",
    icon: "💳",
    content: `
      <p><strong>Gérez vos transactions financières</strong> avec support multi-devises (MAD, USD, EUR, GBP)</p>
      <p><strong>Conversion automatique :</strong> Tous les montants sont convertis en temps réel selon les taux actuels</p>
      <p><strong>Catégories :</strong> Choisissez parmi des catégories prédéfinies ou créez la vôtre</p>
      <p><strong>Recherche & filtres :</strong> Trouvez rapidement vos transactions</p>
    `
  },
  
  invoices: {
    title: "Factures",
    icon: "🧾",
    content: `
      <p><strong>Générez et gérez vos factures clients/fournisseurs</strong> avec suivi du statut (Payée, En attente, Retardée).</p>
      <p><strong>Modèles personnalisables :</strong> Ajoutez votre logo, conditions de paiement, notes.</p>
      <p><strong>Export PDF :</strong> Téléchargez ou imprimez directement vos factures.</p>
      <p><strong>Notifications :</strong> Alertes automatiques pour les factures en retard.</p>
      <p><strong>Intégration comptable :</strong> Les factures sont liées aux transactions et au bilan.</p>
    `
  },

  inventory: {
    title: "Inventaire",
    icon: "📦",
    content: `
      <p><strong>Logique :</strong> Quantité × Prix unitaire = Valeur totale</p>
      <p><strong>Exemple :</strong> 10 unités × 15 د.م./unité = 150 د.م. total</p>
      <p><strong>Multi-devises :</strong> Le prix unitaire peut être en MAD, USD, EUR ou GBP</p>
      <p><strong>Valeur consolidée :</strong> La valeur totale de l’inventaire est affichée en MAD</p>
    `
  },
  dataEntry: {
    title: "Saisie de Données",
    icon: "📝",
    content: `
      <p><strong>1. Créer une saisie :</strong> Choisissez le type (Transactions, Factures, Inventaire ou Personnalisé)</p>
      <p><strong>2. Saisir les données :</strong> Cliquez sur les cellules pour les modifier, utilisez Entrée pour valider</p>
      <p><strong>3. Intégrer au système :</strong> Utilisez les boutons "Créer [Type]" pour ajouter vos données au système principal</p>
      <p><strong>4. Exporter :</strong> Sauvegardez vos données en CSV pour utilisation externe</p>
    `
  },
  
  reports: {
    title: "Rapports",
    icon: "📈",
    content: `
      <p><strong>Accédez à des rapports financiers détaillés</strong> : Bilan, Compte de résultat, Flux de trésorerie.</p>
      <p><strong>Filtres avancés :</strong> Par période, catégorie, projet, utilisateur.</p>
      <p><strong>Export Excel/CSV :</strong> Pour analyse externe ou partage avec votre comptable.</p>
      <p><strong>Graphiques interactifs :</strong> Visualisez les tendances sur plusieurs périodes.</p>
      <p><strong>Planification :</strong> Programmez l'envoi automatique des rapports par email.</p>
    `
  },

  users: {
    title: "Utilisateurs",
    icon: "👥",
    content: `
      <p><strong>Gérer les utilisateurs du système</strong> — réservé aux administrateurs.</p>
      <p><strong>Rôles disponibles :</strong> admin, manager, user</p>
      <p><strong>Permissions :</strong></p>
      <ul class="list-disc list-inside space-y-1">
        <li><strong>Admin :</strong> Peut créer, modifier, supprimer tous les utilisateurs</li>
        <li><strong>Manager/User :</strong> Accès restreint — ne voit pas cette section</li>
      </ul>
      <p><strong>Statut :</strong> Actif ou Inactif — désactive temporairement un utilisateur sans le supprimer</p>
      <p><strong>Sécurité :</strong> Chaque utilisateur a un mot de passe unique et un rôle défini.</p>
    `
  },

  profile: {
    title: "Profil Utilisateur",
    icon: "👤",
    content: `
      <p><strong>Gérez vos informations personnelles</strong> : Nom, email, photo de profil, préférences.</p>
      <p><strong>Langue & Thème :</strong> Choisissez entre français/arabe, mode clair/sombre.</p>
      <p><strong>Historique d’activité :</strong> Consultez vos dernières actions dans le système.</p>
      <p><strong>Notifications :</strong> Configurez vos alertes (email, push, sonores).</p>
      <p><strong>Sécurité :</strong> Modifiez votre mot de passe ou activez l’authentification à deux facteurs (2FA).</p>
    `
  },
  changePassword: {
    title: "Changer de Mot de Passe",
    icon: "🔐",
    content: `
      <p><strong>Sécurisez votre compte</strong> en mettant à jour régulièrement votre mot de passe.</p>
      <p><strong>Exigences de sécurité :</strong> Minimum 8 caractères, incluant majuscule, chiffre et symbole.</p>
      <p><strong>Confirmation :</strong> Entrez votre ancien mot de passe pour valider le changement.</p>
      <p><strong>Validation instantanée :</strong> Le système vérifie la force du nouveau mot de passe.</p>
      <p><strong>Alerte de sécurité :</strong> Vous serez déconnecté après modification pour plus de sécurité.</p>
    `
  },
  login: {
    title: "Connexion",
    icon: "🚪",
    content: `
      <p><strong>Accédez à votre espace sécurisé</strong> avec votre email et mot de passe.</p>
      <p><strong>Récupération de mot de passe :</strong> Cliquez sur "Mot de passe oublié" pour recevoir un lien de réinitialisation.</p>
      <p><strong>Authentification à deux facteurs (2FA) :</strong> Optionnelle mais recommandée pour une sécurité renforcée.</p>
      <p><strong>Session persistante :</strong> Cochez "Se souvenir de moi" pour rester connecté.</p>
      <p><strong>Erreurs courantes :</strong> Vérifiez votre email/mot de passe. Contactez l’admin si bloqué.</p>
    `
  }
};