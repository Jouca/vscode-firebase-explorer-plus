# Firebase Explorer Plus

A VS Code extension to explore and manage Firebase projects with enhanced features.

![Firebase Explorer Plus logo](assets/icon.png)

## About

This extension is a [fork](https://github.com/lazy-orange/vscode-firebase-explorer) of a [fork](https://github.com/jsayol/vscode-firebase-explorer) with significant enhancements, stability and new features for better Firebase management.

All credits to [@lazy-orange](https://github.com/lazy-orange) and [@jsayol](https://github.com/jsayol) (original creator) for their work!

## Installation

Launch VS Code Quick Open (`Ctrl+P`), paste the following command, and press enter:
```
ext install jouca.firebase-explorer-plus
```

Or search for "Firebase Explorer Plus" in the Extensions view.

## ✨ New Features

### 🔥 Firestore Enhancements

#### **Virtual File System for Firestore**
- Edit documents directly in VS Code
- Full JSON editing experience with syntax highlighting

#### **Document Management**
- **Create documents** with 3 methods:
  - Empty document
  - Document with sample data
  - Auto-generated random 20-character IDs
- **Delete documents** with single-click and restore capability
- Full CRUD operations support

#### **Collection Management**
- Create new collections
- Delete collections
- Browse and manage nested subcollections

#### **Database Export/Import**
- Export entire database or individual collections to JSON
- Import from JSON with automatic format detection

#### **Database Operations**
- Clear entire database with double confirmation

### ⚙️ General Improvements

- **Settings button** in all Firebase Explorer views
- Better error handling and user feedback

## Preview

![Firebase Explorer Plus screenshot](images/screenshot.png)
![Firebase Explorer Plus screenshot2](images/screenshot2.png)

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.