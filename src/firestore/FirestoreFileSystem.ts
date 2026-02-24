import * as vscode from 'vscode';
import { FirestoreAPI } from './api';
import { AccountInfo } from '../accounts';
import { FirebaseProject } from '../projects/ProjectManager';
import { providerStore } from '../stores';
import { FirestoreProvider } from './FirestoreProvider';

interface FirestoreDocumentMetadata {
  accountInfo: AccountInfo;
  project: FirebaseProject;
  documentPath: string;
  deleted?: boolean;
}

export class FirestoreFileSystemProvider implements vscode.FileSystemProvider {
  private _emitter = new vscode.EventEmitter<vscode.FileChangeEvent[]>();
  private _documents = new Map<string, { content: Uint8Array; metadata: FirestoreDocumentMetadata }>();

  readonly onDidChangeFile: vscode.Event<vscode.FileChangeEvent[]> = this._emitter.event;

  watch(_uri: vscode.Uri): vscode.Disposable {
    // Ignore, fires for all changes
    return new vscode.Disposable(() => {});
  }

  stat(uri: vscode.Uri): vscode.FileStat {
    const doc = this._documents.get(uri.toString());
    if (!doc) {
      throw vscode.FileSystemError.FileNotFound(uri);
    }

    return {
      type: vscode.FileType.File,
      ctime: Date.now(),
      mtime: Date.now(),
      size: doc.content.byteLength
    };
  }

  readDirectory(_uri: vscode.Uri): [string, vscode.FileType][] {
    return [];
  }

  createDirectory(_uri: vscode.Uri): void {
    // Not supported
  }

  readFile(uri: vscode.Uri): Uint8Array {
    const doc = this._documents.get(uri.toString());
    if (!doc) {
      throw vscode.FileSystemError.FileNotFound(uri);
    }
    return doc.content;
  }

  async writeFile(
    uri: vscode.Uri,
    content: Uint8Array,
    _options: { create: boolean; overwrite: boolean }
  ): Promise<void> {
    const doc = this._documents.get(uri.toString());
    if (!doc) {
      throw vscode.FileSystemError.FileNotFound(uri);
    }

    try {
      // Parse the JSON content
      const jsonString = Buffer.from(content).toString('utf8');
      const jsonData = JSON.parse(jsonString);

      // Convert to Firestore field format
      const fields = convertToFirestoreFields(jsonData);

      // Update or create Firestore document
      const api = FirestoreAPI.for(doc.metadata.accountInfo, doc.metadata.project);
      
      if (doc.metadata.deleted) {
        // Document was deleted, recreate it
        const pathParts = doc.metadata.documentPath.split('/');
        const documentId = pathParts.pop()!;
        const collectionPath = pathParts.join('/');
        
        await api.createDocument(collectionPath, documentId, fields);
        
        // Mark as not deleted anymore
        doc.metadata.deleted = false;
        
        // Update content to clean version
        const cleanContent = JSON.stringify(jsonData, null, 2);
        doc.content = Buffer.from(cleanContent, 'utf8');
        
        vscode.window.showInformationMessage(`Document recreated in Firestore: ${documentId}`);
      } else {
        // Normal update
        await api.updateDocument(doc.metadata.documentPath, fields);
        
        // Update local cache
        doc.content = content;
        
        vscode.window.showInformationMessage(`Document updated in Firestore: ${doc.metadata.documentPath.split('/').pop()}`);
      }

      // Refresh the Firestore tree view
      const firestoreProvider = providerStore.get<FirestoreProvider>('firestore');
      firestoreProvider.refresh();

      this._emitter.fire([{ type: vscode.FileChangeType.Changed, uri }]);
    } catch (err: any) {
      vscode.window.showErrorMessage(`Failed to save Firestore document: ${err.message}`);
      throw vscode.FileSystemError.Unavailable(uri);
    }
  }

  delete(_uri: vscode.Uri): void {
    // Not supported
  }

  rename(_oldUri: vscode.Uri, _newUri: vscode.Uri): void {
    // Not supported
  }

  registerDocument(
    uri: vscode.Uri,
    content: string,
    metadata: FirestoreDocumentMetadata
  ): void {
    this._documents.set(uri.toString(), {
      content: Buffer.from(content, 'utf8'),
      metadata
    });
  }

  markDocumentAsDeleted(projectId: string, documentPath: string): void {
    // Find the document URI by matching project and path
    for (const [uriString, doc] of this._documents.entries()) {
      if (doc.metadata.project.projectId === projectId && 
          doc.metadata.documentPath === documentPath) {
        doc.metadata.deleted = true;
        
        const uri = vscode.Uri.parse(uriString);
        
        // Find the text editor with this document
        const editor = vscode.window.visibleTextEditors.find(
          e => e.document.uri.toString() === uriString
        );
        
        if (editor) {
          // Apply an edit that adds and removes a space at the end
          // This creates the dirty state without visible change
          const edit = new vscode.WorkspaceEdit();
          const lastLine = editor.document.lineCount - 1;
          const lastChar = editor.document.lineAt(lastLine).text.length;
          const position = new vscode.Position(lastLine, lastChar);
          
          // Insert a space
          edit.insert(uri, position, ' ');
          
          vscode.workspace.applyEdit(edit).then(() => {
            // Immediately delete the space to make it invisible
            const deleteEdit = new vscode.WorkspaceEdit();
            deleteEdit.delete(uri, new vscode.Range(position, new vscode.Position(lastLine, lastChar + 1)));
            vscode.workspace.applyEdit(deleteEdit);
          });
        }
        
        break;
      }
    }
  }

  unregisterDocument(uri: vscode.Uri): void {
    this._documents.delete(uri.toString());
  }
}

export function convertToFirestoreFields(data: any): any {
  const fields: any = {};

  for (const [key, value] of Object.entries(data)) {
    fields[key] = convertValueToFirestoreField(value);
  }

  return fields;
}

function convertValueToFirestoreField(value: any): any {
  if (value === null) {
    return { nullValue: null };
  }

  if (typeof value === 'boolean') {
    return { booleanValue: value };
  }

  if (typeof value === 'number') {
    if (Number.isInteger(value)) {
      return { integerValue: value.toString() };
    } else {
      return { doubleValue: value };
    }
  }

  if (typeof value === 'string') {
    // Check if it's a timestamp string - support multiple formats
    const timestampRegex = /^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(\.\d+)?(Z|[+-]\d{2}:\d{2})?$/;
    if (timestampRegex.test(value)) {
      // Normalize timestamp to ISO 8601 format with timezone
      let normalizedTimestamp = value;
      
      // If no timezone specified, add 'Z' (UTC)
      if (!value.endsWith('Z') && !value.match(/[+-]\d{2}:\d{2}$/)) {
        normalizedTimestamp = value + 'Z';
      }
      
      // Validate it's a valid date
      const date = new Date(normalizedTimestamp);
      if (!isNaN(date.getTime())) {
        return { timestampValue: date.toISOString() };
      }
    }
    
    return { stringValue: value };
  }

  if (Array.isArray(value)) {
    return {
      arrayValue: {
        values: value.map(item => convertValueToFirestoreField(item))
      }
    };
  }

  if (typeof value === 'object') {
    // Check for geopoint pattern (object with latitude AND longitude)
    if (
      value.latitude !== undefined && 
      value.longitude !== undefined &&
      typeof value.latitude === 'number' &&
      typeof value.longitude === 'number' &&
      Object.keys(value).length === 2
    ) {
      return {
        geoPointValue: {
          latitude: value.latitude,
          longitude: value.longitude
        }
      };
    }

    // Check for special Firestore types with _type marker
    if (value._type === 'geopoint' && value.latitude !== undefined && value.longitude !== undefined) {
      return {
        geoPointValue: {
          latitude: value.latitude,
          longitude: value.longitude
        }
      };
    }

    if (value._type === 'reference' && value.path !== undefined) {
      return { referenceValue: value.path };
    }

    // Regular object - convert to map
    return {
      mapValue: {
        fields: convertToFirestoreFields(value)
      }
    };
  }

  // Fallback
  return { stringValue: String(value) };
}

// Singleton instance
let firestoreFileSystemProvider: FirestoreFileSystemProvider | undefined;

export function getFirestoreFileSystemProvider(): FirestoreFileSystemProvider {
  if (!firestoreFileSystemProvider) {
    firestoreFileSystemProvider = new FirestoreFileSystemProvider();
  }
  return firestoreFileSystemProvider;
}
