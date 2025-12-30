const path = require("node:path");
const fs = require("node:fs");
const { rootNodeFromAnchor } = require("@codama/nodes-from-anchor");
const { readJson } = require("@codama/renderers-core");
const { visit, rootNodeVisitor } = require("@codama/visitors-core");
const { getRenderMapVisitor } = require("@codama/renderers-vixen-parser");
const { renderVisitor: renderRustVisitor } = require("@codama/renderers-rust");

/**
 * This script generates the Yellowstone Vixen parser from the idl.json file.
 * To run it: node generate-parser.js
 */

const projectName = "pump-parser-gen";
const projectFolder = path.join(__dirname, projectName);
const idlPath = path.join(__dirname, "idls/pumpfun_idl.json");

if (!fs.existsSync(idlPath)) {
    console.error("Error: idl.json not found in the current directory.");
    process.exit(1);
}

const idl = readJson(idlPath);
const node = rootNodeFromAnchor(idl);

// Custom generation logic to handle the Vixen renderer
const generator = rootNodeVisitor((root) => {
    console.log(`Generating Rust SDK and Parser in ${projectFolder}...`);

    // 1. Generate the base Rust SDK
    // This uses the standard Rust renderer which provides the Borsh structs for accounts and instructions.
    const rustVisitor = renderRustVisitor(path.join(projectFolder, "src", "generated_sdk"), {
        crateFolder: projectFolder,
        formatCode: false, // Set to true if you have rustfmt installed and want formatted code
    });
    visit(root, rustVisitor);

    // 2. Generate the Vixen-specific Parser logic
    // This produces the 'instructions_parser.rs' and 'accounts_parser.rs' that Vixen needs.
    const vixenVisitor = getRenderMapVisitor({
        projectFolder,
        projectName,
    });
    const renderMap = visit(root, vixenVisitor);

    // 3. Write the files to disk manually
    // We do this manually to ensure all files are written correctly to the target folder.
    renderMap.forEach((content, fileName) => {
        const filePath = path.join(projectFolder, fileName);
        fs.mkdirSync(path.dirname(filePath), { recursive: true });
        if (content !== undefined) {
            fs.writeFileSync(filePath, content);
        }
    });

    console.log("Success! The parser has been generated.");
    console.log(`Location: ${projectFolder}`);
});

visit(node, generator);
