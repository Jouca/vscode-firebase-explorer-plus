import * as vscode from 'vscode';
import { providerStore } from '../stores';
import {
  FirestoreProviderItem,
  FirestoreProvider,
  DocumentFieldItem,
  DocumentItem,
  CollectionItem
} from './FirestoreProvider';
import { getFieldValue, FirestoreAPI, DocumentFieldValue } from './api';
import { getFullPath, getContext } from '../utils';
import { getFirestoreFileSystemProvider, convertToFirestoreFields } from './FirestoreFileSystem';
import { AccountInfo as accountInfo } from '../accounts';
import { FirebaseProject } from '../projects/ProjectManager';

export function registerFirestoreCommands(context: vscode.ExtensionContext) {
  context.subscriptions.push(
    vscode.commands.registerCommand(
      'firebaseExplorer.firestore.refresh',
      providerRefresh
    )
  );

  context.subscriptions.push(
    vscode.commands.registerCommand(
      'firebaseExplorer.firestore.refreshCollection',
      providerRefresh
    )
  );

  context.subscriptions.push(
    vscode.commands.registerCommand(
      'firebaseExplorer.firestore.copyItemName',
      copyItemName
    )
  );

  context.subscriptions.push(
    vscode.commands.registerCommand(
      'firebaseExplorer.firestore.copyItemPath',
      copyItemPath
    )
  );

  context.subscriptions.push(
    vscode.commands.registerCommand(
      'firebaseExplorer.firestore.copySnippet.JS.ref',
      copySnippetJS_ref
    )
  );

  context.subscriptions.push(
    vscode.commands.registerCommand(
      'firebaseExplorer.firestore.copySnippet.JS.doc.onSnapshot',
      copySnippetJS_doc_onSnapshot
    )
  );

  context.subscriptions.push(
    vscode.commands.registerCommand(
      'firebaseExplorer.firestore.copySnippet.JS.collection.onSnapshot',
      copySnippetJS_collection_onSnapshot
    )
  );

  context.subscriptions.push(
    vscode.commands.registerCommand(
      'firebaseExplorer.firestore.deleteDocument',
      deleteDocument
    )
  );

  context.subscriptions.push(
    vscode.commands.registerCommand(
      'firebaseExplorer.firestore.addDocument',
      addDocument
    )
  );

  context.subscriptions.push(
    vscode.commands.registerCommand(
      'firebaseExplorer.firestore.addCollection',
      addCollection
    )
  );

  context.subscriptions.push(
    vscode.commands.registerCommand(
      'firebaseExplorer.firestore.deleteCollection',
      deleteCollection
    )
  );

  context.subscriptions.push(
    vscode.commands.registerCommand(
      'firebaseExplorer.firestore.exportDatabase',
      exportDatabase
    )
  );

  context.subscriptions.push(
    vscode.commands.registerCommand(
      'firebaseExplorer.firestore.importDatabase',
      importDatabase
    )
  );

  context.subscriptions.push(
    vscode.commands.registerCommand(
      'firebaseExplorer.firestore.refreshDocument',
      providerRefresh
    )
  );

  context.subscriptions.push(
    vscode.commands.registerCommand(
      'firebaseExplorer.firestore.refreshDocumentField',
      providerRefresh
    )
  );

  context.subscriptions.push(
    vscode.commands.registerCommand(
      'firebaseExplorer.firestore.copyDocumentContent',
      copyDocumentContent
    )
  );

  context.subscriptions.push(
    vscode.commands.registerCommand(
      'firebaseExplorer.firestore.viewDocumentContent',
      viewDocumentContent
    )
  );

  context.subscriptions.push(
    vscode.commands.registerCommand(
      'firebaseExplorer.firestore.copyDocumentFieldName',
      copyDocumentFieldName
    )
  );

  context.subscriptions.push(
    vscode.commands.registerCommand(
      'firebaseExplorer.firestore.copyDocumentFieldValue',
      copyDocumentFieldValue
    )
  );

  context.subscriptions.push(
    vscode.commands.registerCommand(
      'firebaseExplorer.firestore.clearDatabase',
      clearDatabase
    )
  );
}

function providerRefresh(element?: FirestoreProviderItem): void {
  const firestoreProvider = providerStore.get<FirestoreProvider>('firestore');
  firestoreProvider.refresh(element);
}

function copyItemName(element: CollectionItem | DocumentItem): void {
  if (!element) {
    return;
  }

  vscode.env.clipboard.writeText(element.name);
}

function copyItemPath(element: CollectionItem | DocumentItem): void {
  if (!element) {
    return;
  }

  vscode.env.clipboard.writeText(
    '/' + getFullPath(element.parentPath, element.name)
  );
}

function copySnippetJS_ref(element: CollectionItem | DocumentItem): void {
  if (!element) {
    return;
  }

  const method = element instanceof CollectionItem ? 'collection' : 'doc';
  const fullPath = getFullPath(element.parentPath, element.name);
  vscode.env.clipboard.writeText(
    `firebase.firestore().${method}('${fullPath}')`
  );
}

function copySnippetJS_doc_onSnapshot(element: DocumentItem): void {
  if (!element) {
    return;
  }

  const fullPath = getFullPath(element.parentPath, element.name);
  vscode.env.clipboard.writeText(
    [
      `const ref = firebase.firestore().doc('${fullPath}');`,
      `ref.onSnapshot((doc) => {`,
      `  const data = doc.data();`,
      `  // ...`,
      `});`
    ].join('\n')
  );
}

function copySnippetJS_collection_onSnapshot(element: CollectionItem): void {
  if (!element) {
    return;
  }

  const fullPath = getFullPath(element.parentPath, element.name);
  vscode.env.clipboard.writeText(
    [
      `const ref = firebase.firestore().collection('${fullPath}');`,
      `ref.onSnapshot((snapshot) => {`,
      `  snapshot.forEach((doc) => {`,
      `    const data = doc.data();`,
      `    // ...`,
      `  });`,
      `});`
    ].join('\n')
  );
}

async function copyDocumentContent(element: DocumentItem): Promise<void> {
  if (!element) {
    return;
  }

  // Documents that have been deleted don't have a "createTime" property
  if (element.document.createTime && !element.document.fields) {
    element.document = await vscode.window.withProgress(
      {
        title: 'Fetching document contents...',
        location: vscode.ProgressLocation.Notification
      },
      async () => {
        const api = FirestoreAPI.for(element.accountInfo, element.project);
        const docPath = getFullPath(element.parentPath, element.name);
        return api.getDocument(docPath);
      }
    );
  }

  if (element.document.fields) {
    const fields = element.document.fields;

    const value = Object.keys(fields).reduce(
      (result, key) => {
        result[key] = getFieldValue(fields[key]);
        return result;
      },
      {} as { [k: string]: any }
    );

    return vscode.env.clipboard.writeText(JSON.stringify(value, null, 2));
  }
}

async function viewDocumentContent(element: DocumentItem): Promise<void> {
  if (!element) {
    return;
  }

  // Documents that have been deleted don't have a "createTime" property
  if (element.document.createTime && !element.document.fields) {
    element.document = await vscode.window.withProgress(
      {
        title: 'Fetching document contents...',
        location: vscode.ProgressLocation.Notification
      },
      async () => {
        const api = FirestoreAPI.for(element.accountInfo, element.project);
        const docPath = getFullPath(element.parentPath, element.name);
        return api.getDocument(docPath);
      }
    );
  }

  if (element.document.fields) {
    const fields = element.document.fields;

    console.log('[Document Debug] Raw fields from Firestore:', JSON.stringify(fields, null, 2));

    const value = Object.keys(fields).reduce(
      (result, key) => {
        result[key] = getFieldValue(fields[key]);
        return result;
      },
      {} as { [k: string]: any }
    );

    const jsonContent = JSON.stringify(value, null, 2);
    const docPath = getFullPath(element.parentPath, element.name);
    
    // Create a Firestore URI
    const uri = vscode.Uri.parse(`firestore:///${element.project.projectId}/${docPath}.json`);
    
    // Register the document with the file system provider
    const firestoreFS = getFirestoreFileSystemProvider();
    firestoreFS.registerDocument(uri, jsonContent, {
      accountInfo: element.accountInfo,
      project: element.project,
      documentPath: docPath
    });
    
    // Open the document
    const doc = await vscode.workspace.openTextDocument(uri);
    await vscode.window.showTextDocument(doc, {
      preview: false,
      viewColumn: vscode.ViewColumn.Active
    });
  } else {
    vscode.window.showInformationMessage('This document has no fields.');
  }
}

function generateRandomId(length: number = 20): string {
  const chars = 'ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789';
  let result = '';
  for (let i = 0; i < length; i++) {
    result += chars.charAt(Math.floor(Math.random() * chars.length));
  }
  return result;
}

async function addCollection(): Promise<void> {
  const context = getContext();
  const account = context.globalState.get<accountInfo>('selectedAccount');
  const project = context.globalState.get<FirebaseProject | null>('selectedProject');

  if (!account || !project) {
    vscode.window.showErrorMessage('Please select a Firebase account and project first.');
    return;
  }

  // Ask for collection name
  const collectionName = await vscode.window.showInputBox({
    prompt: 'Enter collection name',
    placeHolder: 'my-collection',
    validateInput: (value) => {
      if (!value || value.trim() === '') {
        return 'Collection name cannot be empty';
      }
      if (!/^[a-zA-Z0-9_-]+$/.test(value)) {
        return 'Collection name can only contain letters, numbers, underscores and hyphens';
      }
      return undefined;
    }
  });

  if (!collectionName) {
    return;
  }

  // Ask for first document ID
  const documentId = await vscode.window.showInputBox({
    prompt: 'Enter first document ID (leave empty for auto-generated ID)',
    placeHolder: 'my-document-id',
    validateInput: (value) => {
      if (value && !/^[a-zA-Z0-9_-]+$/.test(value)) {
        return 'Document ID can only contain letters, numbers, underscores and hyphens';
      }
      return undefined;
    }
  });

  // User cancelled
  if (documentId === undefined) {
    return;
  }

  const finalDocId = documentId || generateRandomId(20);

  // Ask for creation method
  const creationMethod = await vscode.window.showQuickPick(
    [
      { label: 'Template', description: 'Use a predefined JSON template', value: 'template' },
      { label: 'JSON', description: 'Enter document as JSON object', value: 'json' },
      { label: 'Field by Field', description: 'Add fields one by one with type selection', value: 'fields' }
    ],
    {
      placeHolder: 'Choose how to create the document'
    }
  );

  if (!creationMethod) {
    return;
  }

  let data: any;

  if (creationMethod.value === 'template') {
    // Simple default template
    data = { default: "hello world!" };
  } else if (creationMethod.value === 'json') {
    // Ask for document content as JSON
    const jsonContent = await vscode.window.showInputBox({
      prompt: 'Enter document content as JSON',
      placeHolder: '{"name": "John", "age": 30}',
      validateInput: (value) => {
        if (!value || value.trim() === '') {
          return 'Document content cannot be empty';
        }
        try {
          JSON.parse(value);
          return undefined;
        } catch (e) {
          return 'Invalid JSON format';
        }
      }
    });

    if (!jsonContent) {
      return;
    }

    try {
      data = JSON.parse(jsonContent);
    } catch (error) {
      vscode.window.showErrorMessage(`Invalid JSON: ${error}`);
      return;
    }
  } else {
    // Field by field creation
    data = {};
    let addingFields = true;

    while (addingFields) {
      const fieldName = await vscode.window.showInputBox({
        prompt: 'Enter field name (leave empty to finish)',
        placeHolder: 'fieldName',
        validateInput: (value) => {
          if (value && !/^[a-zA-Z_][a-zA-Z0-9_]*$/.test(value)) {
            return 'Field name must start with a letter or underscore and contain only letters, numbers and underscores';
          }
          if (value && data.hasOwnProperty(value)) {
            return 'This field already exists';
          }
          return undefined;
        }
      });

      if (!fieldName) {
        addingFields = false;
        break;
      }

      const fieldType = await vscode.window.showQuickPick(
        [
          { label: 'String', description: 'Text value', value: 'string' },
          { label: 'Number', description: 'Numeric value (integer or decimal)', value: 'number' },
          { label: 'Boolean', description: 'true or false', value: 'boolean' },
          { label: 'Timestamp', description: 'Date/time (ISO 8601)', value: 'timestamp' },
          { label: 'Geopoint', description: 'Geographic coordinates', value: 'geopoint' },
          { label: 'Null', description: 'Null value', value: 'null' },
          { label: 'Array', description: 'List of values (JSON format)', value: 'array' },
          { label: 'Map', description: 'Nested object (JSON format)', value: 'map' }
        ],
        {
          placeHolder: `Select type for field "${fieldName}"`
        }
      );

      if (!fieldType) {
        continue;
      }

      let fieldValue: any;

      switch (fieldType.value) {
        case 'string':
          fieldValue = await vscode.window.showInputBox({
            prompt: `Enter value for "${fieldName}" (string)`,
            placeHolder: 'text value'
          });
          if (fieldValue === undefined) continue;
          break;

        case 'number':
          const numValue = await vscode.window.showInputBox({
            prompt: `Enter value for "${fieldName}" (number)`,
            placeHolder: '42 or 3.14',
            validateInput: (value) => {
              if (value && isNaN(Number(value))) {
                return 'Must be a valid number';
              }
              return undefined;
            }
          });
          if (numValue === undefined) continue;
          fieldValue = Number(numValue);
          break;

        case 'boolean':
          const boolValue = await vscode.window.showQuickPick(
            [
              { label: 'true', value: true },
              { label: 'false', value: false }
            ],
            {
              placeHolder: `Select value for "${fieldName}"`
            }
          );
          if (!boolValue) continue;
          fieldValue = boolValue.value;
          break;

        case 'timestamp':
          const timestampValue = await vscode.window.showInputBox({
            prompt: `Enter timestamp for "${fieldName}" (ISO 8601 format)`,
            placeHolder: '2024-01-15T10:30:00Z or leave empty for current time',
            validateInput: (value) => {
              if (value && !/^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}/.test(value)) {
                return 'Must be in ISO 8601 format (YYYY-MM-DDTHH:mm:ss)';
              }
              return undefined;
            }
          });
          if (timestampValue === undefined) continue;
          fieldValue = timestampValue || new Date().toISOString();
          break;

        case 'geopoint':
          const latitude = await vscode.window.showInputBox({
            prompt: `Enter latitude for "${fieldName}"`,
            placeHolder: '48.8566',
            validateInput: (value) => {
              const num = Number(value);
              if (isNaN(num) || num < -90 || num > 90) {
                return 'Latitude must be between -90 and 90';
              }
              return undefined;
            }
          });
          if (latitude === undefined) continue;

          const longitude = await vscode.window.showInputBox({
            prompt: `Enter longitude for "${fieldName}"`,
            placeHolder: '2.3522',
            validateInput: (value) => {
              const num = Number(value);
              if (isNaN(num) || num < -180 || num > 180) {
                return 'Longitude must be between -180 and 180';
              }
              return undefined;
            }
          });
          if (longitude === undefined) continue;

          fieldValue = {
            latitude: Number(latitude),
            longitude: Number(longitude)
          };
          break;

        case 'null':
          fieldValue = null;
          break;

        case 'array':
          const arrayValue = await vscode.window.showInputBox({
            prompt: `Enter array for "${fieldName}" (JSON format)`,
            placeHolder: '[1, 2, 3] or ["a", "b", "c"]',
            validateInput: (value) => {
              if (!value) return undefined;
              try {
                const parsed = JSON.parse(value);
                if (!Array.isArray(parsed)) {
                  return 'Must be a valid JSON array';
                }
                return undefined;
              } catch (e) {
                return 'Invalid JSON format';
              }
            }
          });
          if (arrayValue === undefined) continue;
          fieldValue = arrayValue ? JSON.parse(arrayValue) : [];
          break;

        case 'map':
          const mapValue = await vscode.window.showInputBox({
            prompt: `Enter object for "${fieldName}" (JSON format)`,
            placeHolder: '{"key": "value"}',
            validateInput: (value) => {
              if (!value) return undefined;
              try {
                const parsed = JSON.parse(value);
                if (typeof parsed !== 'object' || Array.isArray(parsed)) {
                  return 'Must be a valid JSON object';
                }
                return undefined;
              } catch (e) {
                return 'Invalid JSON format';
              }
            }
          });
          if (mapValue === undefined) continue;
          fieldValue = mapValue ? JSON.parse(mapValue) : {};
          break;
      }

      data[fieldName] = fieldValue;
    }

    if (Object.keys(data).length === 0) {
      vscode.window.showWarningMessage('Cannot create empty document');
      return;
    }
  }

  try {
    await vscode.window.withProgress(
      {
        title: `Creating collection "${collectionName}" with document "${finalDocId}"...`,
        location: vscode.ProgressLocation.Notification
      },
      async () => {
        const api = FirestoreAPI.for(account, project);
        
        // Convert JSON to Firestore fields format
        const fields = convertToFirestoreFields(data);
        
        const createdDoc = await api.createDocument(collectionName, finalDocId, fields);
        
        // Refresh the Firestore view to show the new collection
        const firestoreProvider = providerStore.get<FirestoreProvider>(
          'firestore'
        );
        firestoreProvider.refresh();
        
        vscode.window.showInformationMessage(`Collection "${collectionName}" created with document "${finalDocId}".`);
        
        // Check if auto-open is enabled
        const config = vscode.workspace.getConfiguration('firebaseExplorer.firestore');
        const autoOpen = config.get<boolean>('autoOpenDocument', true);
        
        if (autoOpen) {
          // Create a DocumentItem to open the newly created document
          const documentItem = new DocumentItem(
            createdDoc,
            collectionName,
            account,
            project
          );
          
          // Open the document in editor
          await viewDocumentContent(documentItem);
        }
      }
    );
  } catch (error) {
    vscode.window.showErrorMessage(`Failed to create collection: ${error}`);
  }
}

async function deleteCollection(element: CollectionItem): Promise<void> {
  if (!element) {
    return;
  }

  const collectionPath = getFullPath(element.parentPath, element.name);

  const confirmation = await vscode.window.showWarningMessage(
    `Delete collection "${element.name}"?\n\n` +
      'This will delete all documents in this collection.\n' +
      'Subcollections will not be deleted.\n\n' +
      `/${collectionPath}`,
    { modal: true },
    'Delete'
  );

  if (confirmation === 'Delete') {
    await vscode.window.withProgress(
      {
        title: `Deleting collection "${element.name}"...`,
        location: vscode.ProgressLocation.Notification
      },
      async () => {
        try {
          const api = FirestoreAPI.for(element.account, element.project);
          
          // List all documents in the collection
          let hasMore = true;
          let pageToken: string | undefined;
          let deletedCount = 0;
          
          while (hasMore) {
            const result = await api.listDocuments(collectionPath, 300, pageToken);
            
            if (result.documents && result.documents.length > 0) {
              // Delete each document
              for (const doc of result.documents) {
                const docPath = doc.name.split('/documents/')[1];
                await api.deleteDocument(docPath);
                deletedCount++;
              }
            }
            
            pageToken = result.nextPageToken;
            hasMore = !!pageToken;
          }
          
          // Refresh the entire Firestore view to remove the deleted collection
          const firestoreProvider = providerStore.get<FirestoreProvider>(
            'firestore'
          );
          firestoreProvider.refresh();
          
          vscode.window.showInformationMessage(
            `Collection "${element.name}" deleted successfully. ${deletedCount} document(s) removed.`
          );
        } catch (error) {
          vscode.window.showErrorMessage(`Failed to delete collection: ${error}`);
        }
      }
    );
  }
}

async function clearDatabase(): Promise<void> {
  const context = getContext();
  const account = context.globalState.get<accountInfo>('selectedAccount');
  const project = context.globalState.get<FirebaseProject | null>('selectedProject');

  if (!account || !project) {
    vscode.window.showErrorMessage('Please select a Firebase account and project first.');
    return;
  }

  // First confirmation
  const firstConfirmation = await vscode.window.showWarningMessage(
    `⚠️ DANGER: Clear entire Firestore database?\n\n` +
    `This will DELETE ALL COLLECTIONS and ALL DOCUMENTS in project "${project.projectId}".\n\n` +
    `It will also COST MONEY if your database has a large number of documents, as Firestore charges for document deletions.\n\n` +
    `This action is IRREVERSIBLE and CANNOT BE UNDONE!`,
    { modal: true },
    'Continue'
  );

  if (firstConfirmation !== 'Continue') {
    return;
  }

  // Second confirmation - require typing project ID
  const projectId = project.projectId;
  const typedConfirmation = await vscode.window.showInputBox({
    prompt: `Type the project ID "${projectId}" to confirm deletion`,
    placeHolder: projectId,
    validateInput: (value) => {
      if (value !== projectId) {
        return `Must type exactly: ${projectId}`;
      }
      return undefined;
    }
  });

  if (typedConfirmation !== projectId) {
    return;
  }

  await vscode.window.withProgress(
    {
      title: 'Clearing Firestore database...',
      location: vscode.ProgressLocation.Notification,
      cancellable: false
    },
    async (progress) => {
      try {
        const api = FirestoreAPI.for(account, project);
        let totalDeleted = 0;

        progress.report({ message: 'Listing root collections...' });
        const rootCollections = await api.listCollections('');

        if (!rootCollections.collectionIds || rootCollections.collectionIds.length === 0) {
          vscode.window.showInformationMessage('Database is already empty.');
          return;
        }

        // Delete each root collection
        for (const collectionId of rootCollections.collectionIds) {
          progress.report({ message: `Deleting collection: ${collectionId}...` });
          
          let hasMore = true;
          let pageToken: string | undefined;

          while (hasMore) {
            const result = await api.listDocuments(collectionId, 1000, pageToken);
            
            if (result.documents && result.documents.length > 0) {
              const BATCH_SIZE = 500;
              
              // Process documents in batches of 500 using batchWrite
              for (let i = 0; i < result.documents.length; i += BATCH_SIZE) {
                const batch = result.documents.slice(i, i + BATCH_SIZE);
                const writes = batch.map((doc: any) => ({
                  delete: doc.name
                }));

                try {
                  await retryWithBackoff(() => (api as any).batchWrite(writes));
                  totalDeleted += writes.length;
                  progress.report({ message: `Deleted ${totalDeleted} documents...` });
                } catch (error) {
                  console.error(`Batch delete failed:`, error);
                  // Fallback to individual deletes
                  for (const doc of batch) {
                    try {
                      const docPath = doc.name.split('/documents/')[1];
                      await retryWithBackoff(() => api.deleteDocument(docPath));
                      totalDeleted++;
                    } catch (err) {
                      console.error(`Failed to delete document:`, err);
                    }
                  }
                }
              }
            }

            pageToken = result.nextPageToken;
            hasMore = !!pageToken;
          }
        }

        // Refresh the Firestore view
        const firestoreProvider = providerStore.get<FirestoreProvider>('firestore');
        firestoreProvider.refresh();

        vscode.window.showInformationMessage(
          `Database cleared successfully! ${totalDeleted} document(s) deleted.`
        );
      } catch (error) {
        vscode.window.showErrorMessage(`Failed to clear database: ${error}`);
      }
    }
  );
}

// Helper function to limit concurrency
async function limitConcurrency<T>(
  items: T[],
  limit: number,
  fn: (item: T) => Promise<any>
): Promise<any[]> {
  const results: any[] = [];
  
  for (let i = 0; i < items.length; i += limit) {
    const batch = items.slice(i, i + limit);
    const batchResults = await Promise.all(batch.map(fn));
    results.push(...batchResults);
  }
  
  return results;
}

// Helper function to retry with exponential backoff
async function retryWithBackoff<T>(
  fn: () => Promise<T>,
  maxRetries = 3,
  baseDelay = 1000
): Promise<T> {
  let lastError: any;
  
  for (let attempt = 0; attempt <= maxRetries; attempt++) {
    try {
      return await fn();
    } catch (error: any) {
      lastError = error;
      
      // Check if it's a network error that might be transient
      const isRetriable = 
        error.message?.includes('socket') ||
        error.message?.includes('ECONNRESET') ||
        error.message?.includes('ETIMEDOUT') ||
        error.message?.includes('TLS');
      
      if (attempt < maxRetries && isRetriable) {
        const delay = baseDelay * Math.pow(2, attempt);
        console.log(`Retry attempt ${attempt + 1}/${maxRetries} after ${delay}ms...`);
        await new Promise(resolve => setTimeout(resolve, delay));
      } else {
        break;
      }
    }
  }
  
  throw lastError;
}

async function exportDatabase(): Promise<void> {
  const context = getContext();
  const account = context.globalState.get<accountInfo>('selectedAccount');
  const project = context.globalState.get<FirebaseProject | null>('selectedProject');

  if (!account || !project) {
    vscode.window.showErrorMessage('Please select a Firebase account and project first.');
    return;
  }

  // Ask user where to save the file
  const saveUri = await vscode.window.showSaveDialog({
    defaultUri: vscode.Uri.file(`firestore-${project.projectId}-${Date.now()}.json`),
    filters: {
      'JSON Files': ['json'],
      'All Files': ['*']
    },
    saveLabel: 'Export Database'
  });

  if (!saveUri) {
    return;
  }

  await vscode.window.withProgress(
    {
      title: 'Exporting Firestore database...',
      location: vscode.ProgressLocation.Notification,
      cancellable: false
    },
    async (progress) => {
      try {
        const api = FirestoreAPI.for(account, project);
        const database: any = {};

        progress.report({ message: 'Listing root collections...' });
        const rootCollections = await api.listCollections('');

        if (!rootCollections.collectionIds || rootCollections.collectionIds.length === 0) {
          vscode.window.showInformationMessage('Database is empty. No data to export.');
          return;
        }

        let totalDocs = 0;

        // Export root collections with higher concurrency (10 at a time) for faster export
        const results = await limitConcurrency(
          rootCollections.collectionIds,
          10,
          async (collectionId: string) => {
            progress.report({ message: `Exporting collection: ${collectionId}...` });
            const collectionData = await retryWithBackoff(() => 
              exportCollection(api, collectionId, progress)
            );
            return { collectionId, collectionData };
          }
        );
        
        // Build database object and count documents
        for (const { collectionId, collectionData } of results) {
          database[collectionId] = collectionData;
          const count = Object.keys(collectionData).length;
          totalDocs += count;
        }

        // Write to file
        const jsonContent = JSON.stringify(database);
        await vscode.workspace.fs.writeFile(saveUri, Buffer.from(jsonContent, 'utf8'));

        vscode.window.showInformationMessage(
          `Database exported successfully! ${totalDocs} document(s) exported to ${saveUri.fsPath}`
        );
      } catch (error) {
        vscode.window.showErrorMessage(`Failed to export database: ${error}`);
      }
    }
  );
}

async function exportCollection(
  api: FirestoreAPI,
  collectionPath: string,
  progress?: vscode.Progress<{ message?: string; increment?: number }>
): Promise<any> {
  const collection: any = {};
  let pageToken: string | undefined;
  let hasMore = true;
  let totalProcessed = 0;

  while (hasMore) {
    // Use listDocumentsWithFields with max pageSize for fewer API calls
    const result: any = await retryWithBackoff(() => 
      (api as any).listDocumentsWithFields(collectionPath, 1000, pageToken)
    );

    if (result.documents && result.documents.length > 0) {
      totalProcessed += result.documents.length;
      if (progress) {
        progress.report({ message: `Collection ${collectionPath}: ${totalProcessed} documents exported...` });
      }

      // Process all documents in parallel for maximum speed
      await Promise.all(result.documents.map(async (doc: any) => {
        const docId = doc.name.split('/').pop()!;
        const docPath = doc.name.split('/documents/')[1];
        
        // Convert fields to JSON
        const docData: any = {
          _fields: {}
        };

        if (doc.fields) {
          for (const [key, value] of Object.entries(doc.fields)) {
            docData._fields[key] = getFieldValue(value as DocumentFieldValue);
          }
        }

        // Check for subcollections
        const subcollections = await retryWithBackoff(() => api.listCollections(docPath));
        if (subcollections.collectionIds && subcollections.collectionIds.length > 0) {
          docData._subcollections = {};
          
          // Export subcollections with higher concurrency (10 at a time)
          const subResults = await limitConcurrency(
            subcollections.collectionIds,
            10,
            async (subCollectionId: string) => {
              const subCollectionPath = `${docPath}/${subCollectionId}`;
              const subData = await retryWithBackoff(() => 
                exportCollection(api, subCollectionPath, progress)
              );
              return { subCollectionId, subData };
            }
          );

          for (const { subCollectionId, subData } of subResults) {
            docData._subcollections[subCollectionId] = subData;
          }
        }

        collection[docId] = docData;
      }));
    }

    pageToken = result.nextPageToken;
    hasMore = !!pageToken;
  }

  return collection;
}

async function importDatabase(): Promise<void> {
  const context = getContext();
  const account = context.globalState.get<accountInfo>('selectedAccount');
  const project = context.globalState.get<FirebaseProject | null>('selectedProject');

  if (!account || !project) {
    vscode.window.showErrorMessage('Please select a Firebase account and project first.');
    return;
  }

  // Ask user to select JSON file
  const fileUris = await vscode.window.showOpenDialog({
    canSelectMany: false,
    filters: {
      'JSON Files': ['json'],
      'All Files': ['*']
    },
    openLabel: 'Import Database'
  });

  if (!fileUris || fileUris.length === 0) {
    return;
  }

  const fileUri = fileUris[0];

  // Confirm import
  const confirmation = await vscode.window.showWarningMessage(
    `Import database from ${fileUri.fsPath}?\n\n` +
      'This will create new documents. Existing documents with the same IDs will be overwritten.',
    { modal: true },
    'Import'
  );

  if (confirmation !== 'Import') {
    return;
  }

  await vscode.window.withProgress(
    {
      title: 'Importing Firestore database...',
      location: vscode.ProgressLocation.Notification,
      cancellable: false
    },
    async (progress) => {
      try {
        // Read file
        const fileContent = await vscode.workspace.fs.readFile(fileUri);
        const jsonContent = Buffer.from(fileContent).toString('utf8');
        const database = JSON.parse(jsonContent);

        const api = FirestoreAPI.for(account, project);
        let totalDocs = 0;

        // Import each collection
        for (const [collectionId, collectionData] of Object.entries(database)) {
          progress.report({ message: `Importing collection: ${collectionId}...` });
          const count = await importCollection(api, collectionId, collectionData as any, progress);
          totalDocs += count;
        }

        // Refresh the Firestore view
        const firestoreProvider = providerStore.get<FirestoreProvider>('firestore');
        firestoreProvider.refresh();

        vscode.window.showInformationMessage(
          `Database imported successfully! ${totalDocs} document(s) imported.`
        );
      } catch (error) {
        vscode.window.showErrorMessage(`Failed to import database: ${error}`);
      }
    }
  );
}

async function importCollection(
  api: FirestoreAPI,
  collectionPath: string,
  collectionData: any,
  progress?: vscode.Progress<{ message?: string; increment?: number }>
): Promise<number> {
  let count = 0;
  const entries = Object.entries(collectionData);
  const totalDocs = entries.length;
  const BATCH_SIZE = 500; // Firestore batch write limit
  const CONCURRENT_BATCHES = 5; // Process 5 batches in parallel

  // Helper function to detect if data is in raw Firestore format (stringValue, mapValue, etc.)
  const isFirestoreRawFormat = (obj: any): boolean => {
    if (!obj || typeof obj !== 'object') return false;
    const keys = Object.keys(obj);
    const firestoreKeys = ['stringValue', 'integerValue', 'doubleValue', 'booleanValue', 'timestampValue', 'mapValue', 'arrayValue', 'nullValue', 'bytesValue', 'referenceValue', 'geoPointValue'];
    return keys.some(key => firestoreKeys.includes(key));
  };

  // Helper function to detect if data contains Firestore format anywhere in the tree (deep check)
  const containsFirestoreRawFormat = (data: any): boolean => {
    if (!data || typeof data !== 'object') return false;
    
    // Check if this object itself is in Firestore format
    if (isFirestoreRawFormat(data)) return true;
    
    // Check arrays
    if (Array.isArray(data)) {
      return data.some(item => containsFirestoreRawFormat(item));
    }
    
    // Check object properties recursively
    return Object.values(data).some(value => containsFirestoreRawFormat(value));
  };

  // Helper function to convert raw Firestore format to simple JSON
  const convertFirestoreRawToJson = (data: any): any => {
    if (!data || typeof data !== 'object') return data;
    
    // Check if this is a Firestore field value object
    if (isFirestoreRawFormat(data)) {
      return getFieldValue(data as DocumentFieldValue);
    }
    
    // Recursively process objects and arrays
    if (Array.isArray(data)) {
      return data.map(convertFirestoreRawToJson);
    }
    
    const result: any = {};
    for (const [key, value] of Object.entries(data)) {
      result[key] = convertFirestoreRawToJson(value);
    }
    return result;
  };

  // Helper function to process a single batch
  const processBatch = async (batch: [string, unknown][]) => {
    const writes: any[] = [];

    // Prepare batch writes for documents (without subcollections)
    for (const [docId, docData] of batch) {
      const data = docData as any;
      
      if (data._fields) {
        // Always convert from raw Firestore format to simple JSON if it contains any Firestore keys
        let fieldsToConvert = data._fields;
        
        // Deep check if any field (at any level) is in raw Firestore format
        if (containsFirestoreRawFormat(fieldsToConvert)) {
          // Convert from raw Firestore format to simple JSON first
          fieldsToConvert = convertFirestoreRawToJson(fieldsToConvert);
          console.log(`[Import] Converted raw Firestore format for document ${docId}`);
        }
        
        const fields = convertToFirestoreFields(fieldsToConvert);
        const documentPath = `projects/${(api as any).projectId}/databases/(default)/documents/${collectionPath}/${docId}`;
        
        writes.push({
          update: {
            name: documentPath,
            fields: fields
          }
        });
      }
    }

    // Execute batch write
    let batchCount = 0;
    if (writes.length > 0) {
      try {
        await (api as any).batchWrite(writes);
        batchCount = writes.length;
      } catch (error) {
        console.error(`Batch write failed for collection ${collectionPath}:`, error);
        // Fallback to individual writes for this batch
        for (const [docId, docData] of batch) {
          const data = docData as any;
          if (data._fields) {
            try {
              // Apply same conversion as batch write
              let fieldsToConvert = data._fields;
              if (containsFirestoreRawFormat(fieldsToConvert)) {
                fieldsToConvert = convertFirestoreRawToJson(fieldsToConvert);
              }
              const fields = convertToFirestoreFields(fieldsToConvert);
              await api.createDocument(collectionPath, docId, fields);
              batchCount++;
            } catch (err) {
              console.error(`Failed to import document ${docId}:`, err);
            }
          }
        }
      }
    }

    // Import subcollections (must be done after parent documents exist)
    for (const [docId, docData] of batch) {
      const data = docData as any;
      if (data._subcollections) {
        for (const [subCollectionId, subCollectionData] of Object.entries(data._subcollections)) {
          const subCollectionPath = `${collectionPath}/${docId}/${subCollectionId}`;
          const subCount = await importCollection(api, subCollectionPath, subCollectionData as any, progress);
          batchCount += subCount;
        }
      }
    }

    return batchCount;
  };

  // Process documents in batches with controlled concurrency
  for (let i = 0; i < entries.length; i += BATCH_SIZE * CONCURRENT_BATCHES) {
    // Process multiple batches in parallel (up to CONCURRENT_BATCHES at a time)
    const batchPromises: Promise<number>[] = [];
    
    for (let j = 0; j < CONCURRENT_BATCHES && (i + j * BATCH_SIZE) < entries.length; j++) {
      const batchStart = i + j * BATCH_SIZE;
      const batch = entries.slice(batchStart, batchStart + BATCH_SIZE);
      batchPromises.push(processBatch(batch));
    }

    const batchCounts = await Promise.all(batchPromises);
    count += batchCounts.reduce((sum, c) => sum + c, 0);
    
    if (progress) {
      progress.report({ message: `Collection ${collectionPath}: ${count}/${totalDocs} documents imported...` });
    }
  }

  return count;
}

async function addDocument(element: CollectionItem): Promise<void> {
  if (!element) {
    return;
  }

  const collectionPath = getFullPath(element.parentPath, element.name);

  // Ask for document ID
  const documentId = await vscode.window.showInputBox({
    prompt: 'Enter document ID (leave empty for auto-generated ID)',
    placeHolder: 'my-document-id',
    validateInput: (value) => {
      if (value && !/^[a-zA-Z0-9_-]+$/.test(value)) {
        return 'Document ID can only contain letters, numbers, underscores and hyphens';
      }
      return undefined;
    }
  });

  // User cancelled
  if (documentId === undefined) {
    return;
  }

  // Use auto-generated ID if empty (20 random characters)
  const finalDocId = documentId || generateRandomId(20);

  // Ask for creation method
  const creationMethod = await vscode.window.showQuickPick(
    [
      { label: 'Template', description: 'Use a predefined JSON template', value: 'template' },
      { label: 'JSON', description: 'Enter document as JSON object', value: 'json' },
      { label: 'Field by Field', description: 'Add fields one by one with type selection', value: 'fields' }
    ],
    {
      placeHolder: 'Choose how to create the document'
    }
  );

  if (!creationMethod) {
    return;
  }

  let data: any;

  if (creationMethod.value === 'template') {
    // Simple default template
    data = { default: "hello world!" };
  } else if (creationMethod.value === 'json') {
    // Ask for document content as JSON
    const jsonContent = await vscode.window.showInputBox({
      prompt: 'Enter document content as JSON',
      placeHolder: '{"name": "John", "age": 30}',
      validateInput: (value) => {
        if (!value || value.trim() === '') {
          return 'Document content cannot be empty';
        }
        try {
          JSON.parse(value);
          return undefined;
        } catch (e) {
          return 'Invalid JSON format';
        }
      }
    });

    if (!jsonContent) {
      return;
    }

    try {
      data = JSON.parse(jsonContent);
    } catch (error) {
      vscode.window.showErrorMessage(`Invalid JSON: ${error}`);
      return;
    }
  } else {
    // Field by field creation
    data = {};
    let addingFields = true;

    while (addingFields) {
      const fieldName = await vscode.window.showInputBox({
        prompt: 'Enter field name (leave empty to finish)',
        placeHolder: 'fieldName',
        validateInput: (value) => {
          if (value && !/^[a-zA-Z_][a-zA-Z0-9_]*$/.test(value)) {
            return 'Field name must start with a letter or underscore and contain only letters, numbers and underscores';
          }
          if (value && data.hasOwnProperty(value)) {
            return 'This field already exists';
          }
          return undefined;
        }
      });

      if (!fieldName) {
        addingFields = false;
        break;
      }

      const fieldType = await vscode.window.showQuickPick(
        [
          { label: 'String', description: 'Text value', value: 'string' },
          { label: 'Number', description: 'Numeric value (integer or decimal)', value: 'number' },
          { label: 'Boolean', description: 'true or false', value: 'boolean' },
          { label: 'Timestamp', description: 'Date/time (ISO 8601)', value: 'timestamp' },
          { label: 'Geopoint', description: 'Geographic coordinates', value: 'geopoint' },
          { label: 'Null', description: 'Null value', value: 'null' },
          { label: 'Array', description: 'List of values (JSON format)', value: 'array' },
          { label: 'Map', description: 'Nested object (JSON format)', value: 'map' }
        ],
        {
          placeHolder: `Select type for field "${fieldName}"`
        }
      );

      if (!fieldType) {
        continue;
      }

      let fieldValue: any;

      switch (fieldType.value) {
        case 'string':
          fieldValue = await vscode.window.showInputBox({
            prompt: `Enter value for "${fieldName}" (string)`,
            placeHolder: 'text value'
          });
          if (fieldValue === undefined) continue;
          break;

        case 'number':
          const numValue = await vscode.window.showInputBox({
            prompt: `Enter value for "${fieldName}" (number)`,
            placeHolder: '42 or 3.14',
            validateInput: (value) => {
              if (value && isNaN(Number(value))) {
                return 'Must be a valid number';
              }
              return undefined;
            }
          });
          if (numValue === undefined) continue;
          fieldValue = Number(numValue);
          break;

        case 'boolean':
          const boolValue = await vscode.window.showQuickPick(
            [
              { label: 'true', value: true },
              { label: 'false', value: false }
            ],
            {
              placeHolder: `Select value for "${fieldName}"`
            }
          );
          if (!boolValue) continue;
          fieldValue = boolValue.value;
          break;

        case 'timestamp':
          const timestampValue = await vscode.window.showInputBox({
            prompt: `Enter timestamp for "${fieldName}" (ISO 8601 format)`,
            placeHolder: '2024-01-15T10:30:00Z or leave empty for current time',
            validateInput: (value) => {
              if (value && !/^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}/.test(value)) {
                return 'Must be in ISO 8601 format (YYYY-MM-DDTHH:mm:ss)';
              }
              return undefined;
            }
          });
          if (timestampValue === undefined) continue;
          fieldValue = timestampValue || new Date().toISOString();
          break;

        case 'geopoint':
          const latitude = await vscode.window.showInputBox({
            prompt: `Enter latitude for "${fieldName}"`,
            placeHolder: '48.8566',
            validateInput: (value) => {
              const num = Number(value);
              if (isNaN(num) || num < -90 || num > 90) {
                return 'Latitude must be between -90 and 90';
              }
              return undefined;
            }
          });
          if (latitude === undefined) continue;

          const longitude = await vscode.window.showInputBox({
            prompt: `Enter longitude for "${fieldName}"`,
            placeHolder: '2.3522',
            validateInput: (value) => {
              const num = Number(value);
              if (isNaN(num) || num < -180 || num > 180) {
                return 'Longitude must be between -180 and 180';
              }
              return undefined;
            }
          });
          if (longitude === undefined) continue;

          fieldValue = {
            latitude: Number(latitude),
            longitude: Number(longitude)
          };
          break;

        case 'null':
          fieldValue = null;
          break;

        case 'array':
          const arrayValue = await vscode.window.showInputBox({
            prompt: `Enter array for "${fieldName}" (JSON format)`,
            placeHolder: '[1, 2, 3] or ["a", "b", "c"]',
            validateInput: (value) => {
              if (!value) return undefined;
              try {
                const parsed = JSON.parse(value);
                if (!Array.isArray(parsed)) {
                  return 'Must be a valid JSON array';
                }
                return undefined;
              } catch (e) {
                return 'Invalid JSON format';
              }
            }
          });
          if (arrayValue === undefined) continue;
          fieldValue = arrayValue ? JSON.parse(arrayValue) : [];
          break;

        case 'map':
          const mapValue = await vscode.window.showInputBox({
            prompt: `Enter object for "${fieldName}" (JSON format)`,
            placeHolder: '{"key": "value"}',
            validateInput: (value) => {
              if (!value) return undefined;
              try {
                const parsed = JSON.parse(value);
                if (typeof parsed !== 'object' || Array.isArray(parsed)) {
                  return 'Must be a valid JSON object';
                }
                return undefined;
              } catch (e) {
                return 'Invalid JSON format';
              }
            }
          });
          if (mapValue === undefined) continue;
          fieldValue = mapValue ? JSON.parse(mapValue) : {};
          break;
      }

      data[fieldName] = fieldValue;
    }

    if (Object.keys(data).length === 0) {
      vscode.window.showWarningMessage('Cannot create empty document');
      return;
    }
  }

  try {
    await vscode.window.withProgress(
      {
        title: `Creating document "${finalDocId}"...`,
        location: vscode.ProgressLocation.Notification
      },
      async () => {
        const api = FirestoreAPI.for(element.account, element.project);
        
        // Convert JSON to Firestore fields format
        const fields = convertToFirestoreFields(data);
        
        const createdDoc = await api.createDocument(collectionPath, finalDocId, fields);
        
        // Refresh the collection to show the new document
        const firestoreProvider = providerStore.get<FirestoreProvider>(
          'firestore'
        );
        firestoreProvider.refresh(element);
        
        vscode.window.showInformationMessage(`Document "${finalDocId}" created successfully.`);
        
        // Check if auto-open is enabled
        const config = vscode.workspace.getConfiguration('firebaseExplorer.firestore');
        const autoOpen = config.get<boolean>('autoOpenDocument', true);
        
        if (autoOpen) {
          // Create a DocumentItem to open the newly created document
          const documentItem = new DocumentItem(
            createdDoc,
            element.parentPath,
            element.account,
            element.project
          );
          
          // Open the document in editor
          await viewDocumentContent(documentItem);
        }
      }
    );
  } catch (error) {
    vscode.window.showErrorMessage(`Failed to create document: ${error}`);
  }
}

async function deleteDocument(element: DocumentItem): Promise<void> {
  if (!element) {
    return;
  }

  const fullPath = getFullPath(element.parentPath, element.name);

  const confirmation = await vscode.window.showWarningMessage(
    `Delete document "${element.name}"?\n\n` +
      'Subcollections will not be deleted.\n\n' +
      `/${fullPath}`,
    { modal: true },
    'Delete'
  );

  if (confirmation === 'Delete') {
    await vscode.window.withProgress(
      {
        title: 'Deleting document...',
        location: vscode.ProgressLocation.Notification
      },
      async () => {
        const api = FirestoreAPI.for(element.accountInfo, element.project);
        const docPath = getFullPath(element.parentPath, element.name);
        await api.deleteDocument(docPath);
        
        // Mark the document as deleted in the file system provider (if open)
        const firestoreFS = getFirestoreFileSystemProvider();
        firestoreFS.markDocumentAsDeleted(element.project.projectId, docPath);
        
        // Refresh the entire Firestore view to remove the deleted document
        const firestoreProvider = providerStore.get<FirestoreProvider>(
          'firestore'
        );
        firestoreProvider.refresh();
        
        vscode.window.showInformationMessage(`Document "${element.name}" deleted successfully.`);
      }
    );
  }
}

function copyDocumentFieldName(element: DocumentFieldItem): void {
  if (!element) {
    return;
  }

  vscode.env.clipboard.writeText(element.name);
}

function copyDocumentFieldValue(element: DocumentFieldItem): void {
  if (!element) {
    return;
  }

  try {
    let value = JSON.stringify(getFieldValue(element.fieldValue));
    vscode.env.clipboard.writeText(value);
  } catch (err) {
    console.error(err);
  }
}
