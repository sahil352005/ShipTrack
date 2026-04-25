-- Sample seed data for testing Grafana dashboards before Spark pipeline runs

INSERT INTO shipments (shipment_id, vehicle_id, warehouse_id, region, latitude, longitude, status, weight_kg, distance_km, is_delayed, created_at, estimated_delivery_hours, actual_delivery_hours, event_timestamp)
VALUES
  ('SHP-000001','VH-001','WH-NYC','north',42.5,-75.2,'delivered',120.5,450.0,false,NOW()-INTERVAL '10 hours',8,7,NOW()-INTERVAL '10 hours'),
  ('SHP-000002','VH-002','WH-LAX','west',36.1,-118.3,'delayed',80.0,900.0,true,NOW()-INTERVAL '20 hours',12,18,NOW()-INTERVAL '20 hours'),
  ('SHP-000003','VH-003','WH-CHI','central',40.2,-90.5,'in_transit',200.0,300.0,false,NOW()-INTERVAL '5 hours',6,NULL,NOW()-INTERVAL '5 hours'),
  ('SHP-000004','VH-004','WH-HOU','south',27.8,-85.1,'delivered',50.0,600.0,false,NOW()-INTERVAL '15 hours',10,9,NOW()-INTERVAL '15 hours'),
  ('SHP-000005','VH-005','WH-PHX','east',37.5,-70.2,'created',30.0,150.0,false,NOW()-INTERVAL '1 hour',4,NULL,NOW()-INTERVAL '1 hour');

INSERT INTO delivery_metrics (shipment_id, region, status, estimated_delivery_hours, actual_delivery_hours, delay_hours, is_delayed, on_time)
VALUES
  ('SHP-000001','north','delivered',8,7,-1.0,false,true),
  ('SHP-000002','west','delayed',12,18,6.0,true,false),
  ('SHP-000004','south','delivered',10,9,-1.0,false,true);

INSERT INTO route_performance (region, window_start, window_end, total_shipments, delayed_shipments, delivered_shipments, avg_delivery_hours, avg_distance_km, on_time_rate)
VALUES
  ('north',NOW()-INTERVAL '1 hour',NOW(),10,1,8,8.5,420.0,90.0),
  ('west',NOW()-INTERVAL '1 hour',NOW(),8,3,4,14.0,850.0,62.5),
  ('central',NOW()-INTERVAL '1 hour',NOW(),12,0,10,6.0,310.0,100.0),
  ('south',NOW()-INTERVAL '1 hour',NOW(),9,2,7,10.5,580.0,77.8),
  ('east',NOW()-INTERVAL '1 hour',NOW(),6,1,4,7.0,200.0,83.3);

-- Insert sample users (all passwords are 'password123')
INSERT INTO users (full_name, email, password_hash, role) VALUES
    ('System Admin', 'admin@logistics.com', '$2b$12$EixZaYVK1fsbw1ZfbX3OXePaWxn96p36XoR7A58E.T2Wl6tE3Z2.q', 'Admin'),
    ('Operations Manager', 'manager@logistics.com', '$2b$12$EixZaYVK1fsbw1ZfbX3OXePaWxn96p36XoR7A58E.T2Wl6tE3Z2.q', 'Manager'),
    ('Data Analyst', 'analyst@logistics.com', '$2b$12$EixZaYVK1fsbw1ZfbX3OXePaWxn96p36XoR7A58E.T2Wl6tE3Z2.q', 'Analyst')
ON CONFLICT (email) DO NOTHING;
