{% snapshot snapshot_customers %}
{{
    config(
      unique_key= 'customer_hkey',
      strategy='check',
      check_cols=['customer_hdiff'],
      updated_at='updated'
    )
}}
select * from {{ ref('stg_raw_customers') }}
{% endsnapshot %}