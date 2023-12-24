from torchvision import transforms
from PIL import Image
import torch
import sys

# Get instruction about how to set up the script from https://gitlab.com/ronctli1012/blog1-pretrained-alexnet-and-visualization
# and https://learnopencv.com/pytorch-for-beginners-image-classification-using-pre-trained-models/
args = sys.argv
filePaths = args[1]

alexnet = torch.load('./Db/alexnet')
alexnet.eval()

transform = transforms.Compose([
    transforms.Resize(256),
    transforms.CenterCrop(224),
    transforms.ToTensor(),
    transforms.Normalize(
        mean=[0.485, 0.456, 0.406],
        std=[0.229, 0.224, 0.225]
    )])

# map the class no. to the corresponding label
with open('./Db/model_classes.txt') as labels:
    classes = [i.strip() for i in labels.readlines()]

filePathList = filePaths.split("@")
for filePath in filePathList:
    img = Image.open('./LocalDir/' + filePath)
    img_t = transform(img)
    batch_t = torch.unsqueeze(img_t, 0)
    out = alexnet(batch_t)

    _, index = torch.max(out, 1)
    percentage = torch.nn.functional.softmax(out, dim=1)[0] * 100.0
    print('Using alexnet, ' + filePath + ' is a ' + classes[index[0]] + ' with ' + str(percentage[index[0]].item()) + ' %')

# sort the probability vector in descending order
# sorted, indices = torch.sort(out, descending=True)
# percentage = torch.nn.functional.softmax(out, dim=1)[0] * 100.0
# results = [(classes[i], percentage[i].item()) for i in indices[0][:5]]
# for i in range(5):
#     print('{}: {:.4f}%'.format(results[i][0], results[i][1]))
