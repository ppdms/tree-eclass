// WebGL2 programs for the navigation-layer compositor. One attribute-free vertex
// shader draws a rectangle in target pixels (y down); `flip` stores render-target
// textures top row first, matching textures uploaded from Canvas2D.
export const quadVertex = `#version 300 es
uniform vec4 quad;
uniform vec2 target;
uniform float flip;
out vec2 px;
void main() {
  vec2 corner = vec2(float(gl_VertexID & 1), float(gl_VertexID >> 1));
  px = quad.xy + corner * quad.zw;
  vec2 clip = px / target * 2.0 - 1.0;
  gl_Position = vec4(clip.x, -flip * clip.y, 0.0, 1.0);
}`;

const header = `#version 300 es
precision highp float;
in vec2 px;
out vec4 result;
`;

/** A premultiplied texture covering `region` (target pixels), scaled by `alpha`. */
export const copyShader = `${header}
uniform sampler2D source;
uniform vec4 region;
uniform float alpha;
void main() {
  result = texture(source, (px - region.xy) / region.zw) * alpha;
}`;

/** Premultiplied solid color. */
export const fillShader = `${header}
uniform vec4 color;
void main() {
  result = color;
}`;

/** The backdrop under a surface: overlay layers drawn so far over the base content. */
export const composeShader = `${header}
uniform sampler2D content;
uniform sampler2D under;
uniform vec2 size;
void main() {
  vec2 uv = px / size;
  vec4 top = texture(under, vec2(uv.x, 1.0 - uv.y));
  result = top + texture(content, uv) * (1.0 - top.a);
}`;

/** 4x box downsample: four bilinear taps average each 4x4 source block. */
export const downShader = `${header}
uniform sampler2D source;
uniform vec2 texel;
void main() {
  vec2 center = px * 4.0;
  result = 0.25 * (
    texture(source, (center + vec2(-1.0, -1.0)) * texel) +
    texture(source, (center + vec2(1.0, -1.0)) * texel) +
    texture(source, (center + vec2(-1.0, 1.0)) * texel) +
    texture(source, (center + vec2(1.0, 1.0)) * texel));
}`;

/** One separable Gaussian pass; paired taps use bilinear filtering to halve the fetches. */
export const blurShader = `${header}
uniform sampler2D source;
uniform vec2 texel;
uniform vec2 direction;
uniform float sigma;
float gauss(float x) {
  return exp(-0.5 * x * x / (sigma * sigma));
}
void main() {
  vec2 uv = px * texel;
  vec4 sum = texture(source, uv);
  float total = 1.0;
  float reach = ceil(3.0 * sigma);
  for (float i = 1.0; i <= 24.0; i += 2.0) {
    if (i > reach) break;
    float a = gauss(i);
    float b = gauss(i + 1.0);
    float offset = (i * a + (i + 1.0) * b) / (a + b);
    vec2 step = direction * texel * offset;
    sum += (texture(source, uv + step) + texture(source, uv - step)) * (a + b);
    total += 2.0 * (a + b);
  }
  result = sum / total;
}`;

// Liquid Glass material for one rounded box, in CSS pixels. The interior shows the
// blurred backdrop under an adaptive tint. A convex bezel refracts the backdrop by
// Snell's law, so distortion is concentrated at the rim. A thin specular rim, an
// optional pointer glow and a soft drop shadow complete it.
export const glassShader = `${header}
uniform sampler2D sharp;
uniform sampler2D frosted;
uniform float dpr;
uniform vec4 region;
uniform vec2 frostSize;
uniform vec4 rect;
uniform float radius;
uniform float bevel;
uniform float depth;
uniform float frost;
uniform vec4 tint;
uniform float adaptive;
uniform float lift;
uniform float rim;
uniform float shade;
uniform float shadow;
uniform float dark;
uniform vec3 glow;

// Signed distance to a rounded box and its gradient (Inigo Quilez, sdgRoundBox).
vec3 roundedBox(vec2 p, vec2 extent, float r) {
  vec2 w = abs(p) - extent + r;
  float g = max(w.x, w.y);
  vec2 q = max(w, 0.0);
  float l = length(q);
  vec2 n = g > 0.0 ? q / max(l, 0.0001) : (w.x > w.y ? vec2(1.0, 0.0) : vec2(0.0, 1.0));
  return vec3((g > 0.0 ? l : g) - r, sign(p) * n);
}

vec3 field(vec2 xy) {
  vec2 extent = rect.zw * 0.5;
  return roundedBox(xy - rect.xy - extent, extent, radius);
}

// The rim keeps more of the sharp backdrop so its refraction stays visible; the face is frosted.
vec3 backdrop(vec2 at, float amount) {
  vec2 local = at * dpr - region.xy;
  vec3 sharpColor = texture(sharp, local / region.zw).rgb;
  vec3 frostColor = texture(frosted, local / (frostSize * 4.0)).rgb;
  return mix(sharpColor, frostColor, amount);
}

// View ray (0, 0, -1) refracted into glass (n = 1.5) through a squircle bezel.
vec2 refraction(vec2 n, float t) {
  float u = 1.0 - t;
  float u3 = u * u * u;
  float slope = min(u3 / pow(max(1.0 - u3 * u, 0.0001), 0.75), 8.0);
  vec3 N = normalize(vec3(n * slope, 1.0));
  float eta = 0.6667;
  float k = 1.0 - eta * eta * (1.0 - N.z * N.z);
  vec3 R = vec3(0.0, 0.0, -eta) + (eta * N.z - sqrt(max(k, 0.0))) * N;
  return R.xy / max(-R.z, 0.2) * depth;
}

vec3 light(vec3 color, vec2 xy, vec2 n, float d, float t) {
  float lum = dot(color, vec3(0.2126, 0.7152, 0.0722));
  color = clamp(mix(vec3(lum), color, 1.4), 0.0, 1.0);
  float contrast = dark > 0.5 ? lum : 1.0 - lum;
  float vertical = clamp((xy.y - rect.y) / rect.w, 0.0, 1.0);
  color = mix(color, tint.rgb, clamp(tint.a + adaptive * contrast, 0.0, 0.96)) + lift * (1.4 - 0.8 * vertical);
  float facing = dot(n, normalize(vec2(-0.45, -0.89)));
  float band = 1.0 - smoothstep(0.0, 1.6, -d);
  float u = 1.0 - t;
  float spec = 0.3 + 0.7 * pow(abs(facing), 3.0);
  color += rim * (band * spec + 0.18 * u * u * max(facing, 0.0));
  color *= 1.0 - shade * u * u * max(-facing, 0.0);
  if (glow.z > 0.0) {
    vec2 g = xy - glow.xy;
    float reach = 0.45 * max(rect.z, rect.w) + 30.0;
    float energy = glow.z * exp(-dot(g, g) / (reach * reach));
    color += energy * ((dark > 0.5 ? 0.07 : 0.12) + 0.6 * rim * band);
  }
  return color;
}

void main() {
  vec2 xy = px / dpr;
  vec3 f = field(xy);
  float d = f.x;
  vec2 n = f.yz / max(length(f.yz), 0.0001);
  float coverage = clamp(0.5 - d * dpr, 0.0, 1.0);
  float s = shadow * (1.0 - smoothstep(-4.0, 16.0, field(xy - vec2(0.0, 4.0)).x));
  if (coverage <= 0.0) {
    result = vec4(0.0, 0.0, 0.0, s);
    return;
  }
  float t = clamp(-d / bevel, 0.0, 1.0);
  vec2 offset = refraction(n, t);
  vec2 at = xy + offset;
  float amount = frost * mix(0.3, 1.0, smoothstep(0.0, 0.8, t));
  vec3 color = vec3(
    backdrop(at, amount).r,
    backdrop(at + offset * 0.05, amount).g,
    backdrop(at + offset * 0.1, amount).b);
  color = light(color, xy, n, d, t);
  result = vec4(color * coverage, coverage + s * (1.0 - coverage));
}`;

/** The scroll edge softens content passing under a floating bar. */
export const scrollEdgeShader = `${header}
uniform sampler2D frosted;
uniform float dpr;
uniform vec4 region;
uniform vec2 frostSize;
uniform float top;
uniform float bottom;
uniform vec4 page;
uniform float strength;
void main() {
  float y = px.y / dpr;
  float a = strength * (1.0 - smoothstep(top, bottom, y));
  vec3 c = mix(texture(frosted, (px - region.xy) / (frostSize * 4.0)).rgb, page.rgb, page.a);
  result = vec4(c * a, a);
}`;
