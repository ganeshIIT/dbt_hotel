

{{ codegen.generate_model_yaml(
    model_names=['stg_raw_reservations']
) }}


{{ codegen.get_models(directory='analytics')}}

{{ codegen.generate_source(schema_name= 'raw', database_name= 'hotel_prod') }}

