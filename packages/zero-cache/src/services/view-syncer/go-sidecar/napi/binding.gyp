{
  "targets": [
    {
      "target_name": "goivm_napi",
      "sources": ["addon.c"],
      "cflags": ["-O2", "-Wall"],
      "conditions": [
        ["OS=='mac'", {
          "xcode_settings": {
            "OTHER_CFLAGS": ["-O2", "-Wall"]
          }
        }]
      ]
    }
  ]
}
