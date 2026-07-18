import unittest

from app.mcp.server import knowledge_mcp


class MCPServerTests(unittest.IsolatedAsyncioTestCase):
    async def test_read_only_tool_contracts_are_flat_and_complete(self):
        tools = {tool.name: tool for tool in await knowledge_mcp.list_tools()}
        self.assertEqual(set(tools), {
            "list_courses", "list_materials", "search_materials", "read_material",
            "get_recent_changes", "get_index_status",
        })
        self.assertIn("query", tools["search_materials"].inputSchema["properties"])
        self.assertIn("document_id", tools["read_material"].inputSchema["properties"])
        self.assertNotIn("source_path", tools["read_material"].inputSchema["properties"])
        locator = tools["read_material"].inputSchema["$defs"]["LocatorInput"]
        self.assertEqual(locator["required"], ["type", "start"])
        self.assertIn("page", locator["properties"]["type"]["description"])
        self.assertIn("25", locator["properties"]["start"]["examples"])

    async def test_resources_use_opaque_document_ids(self):
        templates = {str(item.uriTemplate) for item in await knowledge_mcp.list_resource_templates()}
        self.assertIn("eclass://documents/{document_id}", templates)
        self.assertIn("eclass://documents/{document_id}/units/{locator}", templates)

    async def test_http_transport_allows_lan_reverse_proxy_host(self):
        settings = knowledge_mcp.settings.transport_security
        self.assertTrue(settings.enable_dns_rebinding_protection)
        self.assertIn("uni.lan", settings.allowed_hosts)
        self.assertIn("https://uni.lan", settings.allowed_origins)


if __name__ == "__main__":
    unittest.main()
