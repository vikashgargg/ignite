<template>
  <Layout>
    <template #nav-bar-title-after>
      <div class="ml-2">
        <span class="text-sm font-normal text-gray-500">{{ libVersion }}</span>
      </div>
    </template>
    <template #doc-before>
      <div
        :class="[
          'vp-doc',
          theme.externalLinkIcon && 'external-link-icon-enabled',
        ]"
      >
        <div v-if="!isDevGuide" class="info custom-block !pt-2 !pb-2">
          <p>
            <span class="mr-1">&#11088;</span
            ><span class="font-semibold">Using Zelox?</span> Tell us how it's
            going in
            <a
              href="https://github.com/vikashgargg/zelox/discussions"
              target="_blank"
              >GitHub Discussions</a
            >!
          </p>
        </div>
        <div
          v-if="version !== 'latest' && !isDevGuide"
          class="warning custom-block !pt-2 !pb-2"
        >
          <p>
            This is
            {{
              version === "main" ? "an unreleased version" : "an old version"
            }}
            of Zelox. Please visit
            <a :href="latestBase" target="_blank">here</a>
            for the documentation of the latest stable version.
          </p>
        </div>
        <div
          v-if="version !== 'main' && isDevGuide"
          class="warning custom-block !pt-2 !pb-2"
        >
          <p>
            This is a snapshot of the development guide for a released Zelox
            version. For up-to-date information, please visit the development
            guide for the <code>main</code> branch
            <a :href="`${mainBase}development/`" target="_blank"> here</a>.
          </p>
        </div>
        <div
          v-if="version === 'main' && isDevGuide"
          class="info custom-block !pt-2 !pb-2"
        >
          <p>
            This guide is up-to-date with the
            <code>main</code> branch.
          </p>
        </div>
      </div>
    </template>
  </Layout>
</template>

<script setup lang="ts">
import { useData, useRoute } from "vitepress";
import DefaultTheme from "vitepress/theme";
import { computed } from "vue";

const { Layout } = DefaultTheme;
const { site, theme } = useData();
const route = useRoute();

const version = computed(() => site.value.contentProps?.version);
const libVersion = computed(() => site.value.contentProps?.libVersion);
const isDevGuide = computed(() =>
  route.data.relativePath.startsWith("development/"),
);

const switchBase = (base: string, version: string): string => {
  return base.replace(/\/[^/]+\/$/, `/${version}/`);
};

const latestBase = computed(() => switchBase(site.value.base, "latest"));
const mainBase = computed(() => switchBase(site.value.base, "main"));
</script>
