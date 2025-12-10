
  jsonb_path_query_first(note_attributes::jsonb, '$[*] ? (@.name == "shopifyCartToken")') ->> 'value' AS rudder_shopify_cart_token,
