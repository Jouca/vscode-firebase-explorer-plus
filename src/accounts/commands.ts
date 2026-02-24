import * as vscode from 'vscode';
import { AccountItem, ProjectsProvider } from '../projects/ProjectsProvider';
import { generateNonce, getContext } from '../utils';
import { AccountManager } from './AccountManager';
import { endLogin, initiateLogin } from './login';
import { providerStore } from '../stores';

export function registerAccountsCommands(context: vscode.ExtensionContext) {
  context.subscriptions.push(
    vscode.commands.registerCommand('firebaseExplorer.accounts.add', addAccount)
  );

  context.subscriptions.push(
    vscode.commands.registerCommand(
      'firebaseExplorer.accounts.remove',
      removeAccount
    )
  );

  context.subscriptions.push(
    vscode.commands.registerCommand(
      'firebaseExplorer.accounts.clearAll',
      clearAllAccounts
    )
  );

  context.subscriptions.push(
    vscode.commands.registerCommand(
      'firebaseExplorer.openSettings',
      openSettings
    )
  );
}

async function addAccount(): Promise<void> {
  console.log('Starting addAccount flow...');
  try {
    const account = await vscode.window.withProgress(
      {
        title: 'Waiting for login to complete...',
        location: vscode.ProgressLocation.Notification,
        cancellable: true
      },
      (_, cancelationToken) => {
        const nonce = generateNonce();
        console.log('Generated nonce:', nonce);

        cancelationToken.onCancellationRequested(() => {
          console.log('Login cancelled by user');
          endLogin(nonce);
        });

        return initiateLogin(nonce);
      }
    );

    console.log('Login completed, account:', account);
    if (account) {
      console.log('Saving account to storage...');
      await AccountManager.addAccount(account);
      console.log('Account saved. Triggering refresh...');
      
      // Refresh the projects view directly
      const projectsProvider = providerStore.get<ProjectsProvider>('projects');
      projectsProvider.refresh();
      
      console.log('Refresh triggered');
      vscode.window.showInformationMessage(`Successfully added account: ${account.user.email}`);
    } else {
      console.warn('Account is null/undefined');
      vscode.window.showWarningMessage('Failed to add new account.');
    }
  } catch (err: any) {
    console.error('Error in addAccount:', err);
    vscode.window.showErrorMessage('Failed to add account: ' + (err.message || 'Unknown error'));
  }
}

function removeAccount(element: AccountItem): void {
  const context = getContext();
  const selectedAccount = AccountManager.getSelectedAccountInfo();

  if (selectedAccount === element.accountInfo) {
    context.globalState.update('selectedAccount', undefined);
    context.globalState.update('selectedProject', undefined);
    vscode.commands.executeCommand('firebaseExplorer.functions.refresh');
    vscode.commands.executeCommand('firebaseExplorer.apps.refresh');
    vscode.commands.executeCommand('firebaseExplorer.firestore.refresh');
    vscode.commands.executeCommand('firebaseExplorer.database.refresh');
  }

  AccountManager.removeAccount(element.accountInfo);
  
  // Refresh the projects view directly
  const projectsProvider = providerStore.get<ProjectsProvider>('projects');
  projectsProvider.refresh();
}

async function clearAllAccounts(): Promise<void> {
  const result = await vscode.window.showWarningMessage(
    'Are you sure you want to remove all Firebase accounts?',
    { modal: true },
    'Yes',
    'No'
  );

  if (result === 'Yes') {
    const context = getContext();
    await context.globalState.update('accounts', {});
    await context.globalState.update('selectedAccount', undefined);
    await context.globalState.update('selectedProject', undefined);
    
    // Refresh all providers directly
    const projectsProvider = providerStore.get<ProjectsProvider>('projects');
    projectsProvider.refresh();
    
    vscode.window.showInformationMessage('All Firebase accounts have been removed.');
  }
}

async function openSettings(): Promise<void> {
  await vscode.commands.executeCommand(
    'workbench.action.openSettings',
    '@ext:vymarkov.firebase-explorer'
  );
}
