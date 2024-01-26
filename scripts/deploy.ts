const items = Deno.readDir("./");

const hasWrangler = async () => {
  const wrangler = await Deno.stat("/usr/local/bin/wrangler");
  if (wrangler && wrangler.isFile) {
    return true;
  }
  return false;
};

if (!(await hasWrangler())) {
  console.log("wrangler not found");
  Deno.exit(1);
}

for await (const item of items) {
  if (item.isDirectory) {
    await deploy(item.name);
  }
}

async function deploy(name: string) {
  const items = Deno.readDir(`./${name}`);
  let hasWranglerToml = false;
  for await (const item of items) {
    if (item.name === "wrangler.toml" && item.isFile) {
      hasWranglerToml = true;
    }
  }
  if (!hasWranglerToml) {
    return;
  }
  console.log(`Deploying ${name}`);
  const cmd = new Deno.Command("/usr/local/bin/wrangler", {
    args: ["deploy"],
  });
  const { code } = await cmd.output();
  console.log(`code: ${code}`);
  if (code !== 0) {
    console.log(`Failed to deploy ${name}`);
  }
}
