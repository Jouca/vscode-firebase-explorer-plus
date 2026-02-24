import * as vscode from 'vscode';
import { providerStore } from '../stores';
import {
  FirestoreProviderItem,
  FirestoreProvider,
  DocumentFieldItem,
  DocumentItem,
  CollectionItem
} from './FirestoreProvider';
import { getFieldValue, FirestoreAPI } from './api';
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
