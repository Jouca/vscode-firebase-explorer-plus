import * as vscode from 'vscode';
import { providerStore } from '../stores';
import { FirebaseProject } from '../projects/ProjectManager';
import { ProjectsProvider, AccountItem } from '../projects/ProjectsProvider';
import { FirestoreProvider } from '../firestore/FirestoreProvider';
import { DatabaseProvider } from '../database/DatabaseProvider';
import { setContext, ContextValue, getContext } from '../utils';
import { AppsProvider } from '../apps/AppsProvider';
import { AccountInfo, AccountManager } from '../accounts';
import { FunctionsProvider } from '../functions/FunctionsProvider';
import { HostingProvider } from '../hosting/HostingProvider';

export function registerProjectsCommands(context: vscode.ExtensionContext) {
  context.subscriptions.push(
    vscode.commands.registerCommand(
      'firebaseExplorer.projects.refresh',
      refreshProjects
    )
  );

  context.subscriptions.push(
    vscode.commands.registerCommand(
      'firebaseExplorer.projects.selection',
      projectSelection
    )
  );
}

function projectSelection(
  accountInfo: AccountInfo,
  project: FirebaseProject
): void {
  const context = getContext();
  const currentAccount = context.globalState.get<AccountInfo>(
    'selectedAccount'
  );
  const currentProject = context.globalState.get<FirebaseProject | null>(
    'selectedProject'
  );

  if (accountInfo === currentAccount && project === currentProject) {
    return;
  }

  const hostingProvider = providerStore.get<HostingProvider>('hosting');
  const functionsProvider = providerStore.get<FunctionsProvider>('functions');
  const appsProvider = providerStore.get<AppsProvider>('apps');
  const firestoreProvider = providerStore.get<FirestoreProvider>('firestore');
  const databaseProvider = providerStore.get<DatabaseProvider>('database');

  setContext(ContextValue.ProjectSelected, false);
  setContext(ContextValue.HostingLoaded, false);
  setContext(ContextValue.FunctionsLoaded, false);
  setContext(ContextValue.AppsLoaded, false);
  setContext(ContextValue.FirestoreLoaded, false);
  setContext(ContextValue.DatabaseLoaded, false);

  if (accountInfo && project) {
    // Empty selection and refresh to show "Loading..."
    context.globalState.update('selectedAccount', null);
    context.globalState.update('selectedProject', null);
  }

  hostingProvider.refresh();
  functionsProvider.refresh();
  appsProvider.refresh();
  firestoreProvider.refresh();
  databaseProvider.refresh();

  if (accountInfo && project) {
    setTimeout(() => {
      // Re-populate the treeviews for the selected project
      context.globalState.update('selectedAccount', accountInfo);
      context.globalState.update('selectedProject', project);

      hostingProvider.refresh();
      functionsProvider.refresh();
      appsProvider.refresh();
      firestoreProvider.refresh();
      databaseProvider.refresh();

      setContext(ContextValue.ProjectSelected, !!(accountInfo && project));
    }, 250);
  }
}

async function refreshProjects(element?: AccountItem): Promise<void> {
  console.log('refreshProjects called, element:', element);
  if (!element) {
    const accounts = await AccountManager.getAccounts();
    console.log('Accounts for refresh:', accounts.length);
    if (accounts?.length > 0) {
      const firstAccount = accounts[0];
      if (firstAccount?.info?.user?.email) {
        element = new AccountItem(firstAccount.info);
      }
    }
  }

  const projectsProvider = providerStore.get<ProjectsProvider>('projects');
  console.log('Calling projectsProvider.refresh()');
  projectsProvider.refresh(element);
}
